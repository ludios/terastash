"use strict";

const A                   = require('ayy');
const T                   = require('notmytype');
const fs                  = require('./fs-promisified');
const path                = require('path');
const crypto              = require('crypto');
const chalk               = require('chalk');
const inspect             = require('util').inspect;
const streamifier         = require('streamifier');
const Transform           = require('stream').Transform;
const os                  = require('os');
const utils               = require('./utils');
const commaify            = utils.commaify;
const OutputContextType   = utils.OutputContextType;
const filename            = require('./filename');
const compile_require     = require('./compile_require');
const cassandra           = require('cassandra-driver');
const RetryPolicy         = require('cassandra-driver/lib/policies/retry').RetryPolicy;
const LoadBalancingPolicy = require('cassandra-driver/lib/policies/load-balancing').LoadBalancingPolicy;
const deepEqual           = require('deep-equal');
const Table               = require('cli-table');
const Combine             = require('combine-streams');
const aes                 = require('./aes');
const gcmer               = require('./gcmer');
const hasher              = require('./hasher');
const localfs             = require('./chunker/localfs');
const gdrive              = require('./chunker/gdrive');
const sse4_crc32          = compile_require('sse4_crc32');
const readline            = require('readline');
const padded_stream       = require('./padded_stream');
const line_reader         = require('./line_reader');
const work_stealer        = require('./work_stealer');
const random_stream       = require('./random_stream');
const transit             = require('transit-js');

let TERASTASH_VERSION;
let HOSTNAME;
let USERNAME;
if (Number(process.env.TERASTASH_INSECURE_AND_DETERMINISTIC)) {
	TERASTASH_VERSION = 'test-version';
	HOSTNAME = 'test-hostname';
	USERNAME = 'test-username';
} else {
	TERASTASH_VERSION =
		require('../package.json').version +
		'D' + fs.readFileSync(__dirname + '/date-version').toString('utf-8').trim();
	// TODO: will need to be re-initialized after V8 snapshot
	HOSTNAME = os.hostname();
	// Linux and OS X use USER, Windows uses USERNAME
	USERNAME = process.env.USER || process.env.USERNAME;
}
T(HOSTNAME, T.string);
T(USERNAME, T.string);

const KEYSPACE_PREFIX = "ts_";


class CustomRetryPolicy extends RetryPolicy {
	onReadTimeout(requestInfo, _consistency, _received, _blockFor, _isDataPresent) {
		if (requestInfo.nbRetry > 10) {
			return this.rethrowResult();
		}
		return this.retryResult();
	}

	onWriteTimeout(requestInfo, _consistency, _received, _blockFor, _writeType) {
		if (requestInfo.nbRetry > 10) {
			return this.rethrowResult();
		}
		// We assume it's safe to retry our writes
		return this.retryResult();
	}
}

class RepeatHostPolicy extends LoadBalancingPolicy {
	newQueryPlan(keyspace, queryOptions, callback) {
		const hosts = this.hosts.values();
		callback(null, {next: () => (
			{
				value: hosts[0],
				done: false
			}
		)});
	}
}

function getContactPoint() {
	return process.env.TERASTASH_CASSANDRA_HOST || '127.0.0.1';
}

function getNewClient() {
	return new cassandra.Client({
		contactPoints: [getContactPoint()],
		policies: {
			retry: new CustomRetryPolicy(),
			/**
			 * Use a load balancing policy that doesn't give up when we reach the
			 * last host.  If we don't do this, ts export-db will fail most of the
			 * time on a large database with:
			 *
			 *   Error: All host(s) tried for query failed. First host tried,
			 *   127.0.0.1:9042: ResponseError: Operation timed out -
			 *   received only 0 responses.. See innerErrors.
			 *
			 * The error itself is thrown by RequestHandler.prototype.send
			 * near line "//No connection available".
			 */
			loadBalancing: new RepeatHostPolicy()
		},
		socketOptions: {
			/* Disable the read timeout (default 12000ms) because
			 * otherwise we see ts export-db failures */
			readTimeout: 0
		}
	});
}

const getStashes = utils.makeConfigFileInitializer(
	"stashes.json", {
		stashes: {},
		_comment: utils.ol(`You cannot change the name of a stash because it must match
			the Cassandra keyspace, and you cannot rename a Cassandra keyspace.`)
	}
);

const getChunkStores = utils.makeConfigFileInitializer(
	"chunk-stores.json", {
		stores: {},
		_comments: [
			utils.ol(`You cannot change the name of a store because existing
				chunks reference it by name in the Cassandra database.`),
			utils.ol(`Folders specified in 'parents' for type 'gdrive', and
				'directories' for type 'localfs' *must not* be used by anything but one chunk store.
				('ts fsck' will delete unreferenced chunks in the chunk store's folder.)`),
			utils.ol(`A chunk store *must not* be used by more than one stash.
				('ts fsck' will delete unreferenced chunks in the chunk store's folder.)`)
		]
	}
);

class UsageError extends Error {
	get name() {
		return this.constructor.name;
	}
}

class DirectoryNotEmptyError extends Error {
	get name() {
		return this.constructor.name;
	}
}

class NotInWorkingDirectoryError extends Error {
	get name() {
		return this.constructor.name;
	}
}

class KeyspaceMissingError extends Error {
	get name() {
		return this.constructor.name;
	}
}

const StashInfoType = T.shape({path: T.string});

/**
 * For a given pathname, return a stash that contains the file,
 * or `null` if there is no terastash base.
 */
async function getStashInfoByPath(pathname) {
	T(pathname, T.string);
	const config = await getStashes();
	if (!config.stashes || typeof config.stashes !== "object") {
		throw new Error(`terastash config has no "stashes" or not an object`);
	}

	const resolvedPathname = path.resolve(pathname);
	for (const stashName of Object.keys(config.stashes)) {
		const stash = config.stashes[stashName];
		//console.log(resolvedPathname, stash.path);
		if (resolvedPathname === stash.path || resolvedPathname.startsWith(stash.path + path.sep)) {
			stash.name = stashName;
			return stash;
		}
	}
	throw new NotInWorkingDirectoryError(
		`File ${inspect(pathname)} is not in a terastash working directory`);
}

/**
 * Return a stash for a given stash name
 */
async function getStashInfoByName(stashName) {
	T(stashName, T.string);
	const config = await getStashes();
	if (!config.stashes || typeof config.stashes !== "object") {
		throw new Error(`terastash config has no "stashes" or not an object`);
	}

	const stash = config.stashes[stashName];
	if (!stash) {
		throw new Error(`No stash with name ${stashName}`);
	}
	stash.name = stashName;
	return stash;
}

/**
 * For any given relative user path, which may include ../, return
 * the corresponding path that should be used in the Cassandra
 * database.
 */
function userPathToDatabasePath(base, p) {
	T(base, T.string, p, T.string);
	const resolved = path.resolve(p);
	if (resolved === base) {
		return "";
	} else {
		const dbPath = resolved.replace(base + "/", "").replace(/\\/g, "/");
		A(!dbPath.startsWith('/'), dbPath);
		return dbPath;
	}
}

class DifferentStashesError extends Error {
	get name() {
		return this.constructor.name;
	}
}

async function getStashInfoForPaths(paths) {
	// Make sure all paths are in the same stash
	const stashInfos = [];
	// Don't use Promise.all to avoid having too many file handles open
	for (const p of paths) {
		stashInfos.push(await getStashInfoByPath(path.resolve(p)));
	}
	const stashNames = stashInfos.map(utils.prop('name'));
	if (!utils.allIdentical(stashNames)) {
		throw new DifferentStashesError(
			`All paths used in command must be in the same stash;` +
			` stashes were ${inspect(stashNames)}`);
	}
	return stashInfos[0];
}

async function getStashInfoForNameOrPaths(stashName, paths) {
	T(stashName, T.maybe(T.string), paths, T.list(T.string));
	if (stashName !== null) {
		return await getStashInfoByName(stashName);
	} else {
		return await getStashInfoForPaths(paths);
	}
}

/**
 * If stashName === null, convert p to database path, else just return p.
 */
function eitherPathToDatabasePath(stashName, base, p) {
	T(stashName, T.maybe(T.string), base, T.string, p, T.string);
	if (stashName === null) {
		return userPathToDatabasePath(base, p);
	}
	return p;
}

function isColumnMissingError(err) {
	return /^ResponseError: (Undefined name .* in selection clause|Unknown identifier )/.test(String(err));
}

function isKeyspaceMissingError(err) {
	return /^ResponseError: Keyspace .* does not exist/.test(String(err));
}

/**
 * Run a Cassandra query and return a Promise that is fulfilled
 * with the query results.
 */
function runQuery(client, statement, queryArgs) {
	T(client, cassandra.Client, statement, T.string, queryArgs, T.optional(Array));
	//console.log(`runQuery(${client}, ${inspect(statement)}, ${inspect(queryArgs)})`);
	return new Promise(function runQuery$Promise(resolve, reject) {
		client.execute(statement, queryArgs, {prepare: true}, function(err, result) {
			if (err) {
				reject(err);
			} else {
				resolve(result);
			}
		});
	}).catch(function runQuery$catch(err) {
		if (isKeyspaceMissingError(err)) {
			throw new KeyspaceMissingError(err.message);
		}
		throw err;
	});
}

/**
 * Call function `f` with a Cassandra client and shut down the client
 * after `f` is done.
 */
function doWithClient(client, f) {
	T(client, cassandra.Client, f, T.function);
	const p = f(client);
	function shutdown(ret) {
		try {
			client.shutdown();
		} catch(e) {
			console.error("client.shutdown() failed:");
			console.error(e.stack);
		}
		return ret;
	}
	// This is like a "finally" clause that we use to shut down the client,
	// without yet handling the error returned by `f`, if any
	return p.then(
		shutdown,
		function(e) {
			shutdown();
			throw e;
		}
	);
}

async function doWithPath(stashName, p, fn) {
	T(stashName, T.maybe(T.string), p, T.string, fn, T.function);
	const resolvedPathname = path.resolve(p);
	let dbPath;
	let stashInfo;
	if (stashName) { // Explicit stash name provided
		stashInfo = await getStashInfoByName(stashName);
		dbPath = p;
	} else {
		stashInfo = await getStashInfoByPath(resolvedPathname);
		dbPath = userPathToDatabasePath(stashInfo.path, p);
	}

	const parentPath = utils.getParentPath(dbPath);
	A(!parentPath.startsWith('/'), parentPath);

	// TODO: validate stashInfo.name - it may contain injection
	return fn(stashInfo, dbPath, parentPath);
}

const pathsorterAsc   = utils.comparedBy(row => row.basename);
const pathsorterDesc  = utils.comparedBy(row => row.basename, true);
const mtimeSorterAsc  = utils.comparedBy(row => row.mtime.getTime());
const mtimeSorterDesc = utils.comparedBy(row => row.mtime.getTime(), true);
const sizeSorterAsc   = utils.comparedBy(row => Number(row.size));
const sizeSorterDesc  = utils.comparedBy(row => Number(row.size), true);

class NoSuchPathError extends Error {
	get name() {
		return this.constructor.name;
	}
}

class NotAFileError extends Error {
	get name() {
		return this.constructor.name;
	}
}

class UnexpectedFileError extends Error {
	get name() {
		return this.constructor.name;
	}
}

class FileChangedError extends Error {
	get name() {
		return this.constructor.name;
	}
}

async function getRowByParentBasename(client, stashName, parent, basename, cols) {
	T(client, cassandra.Client, stashName, T.string, parent, Buffer, basename, T.string, cols, utils.ColsType);
	const result = await runQuery(
		client,
		`SELECT ${utils.colsAsString(cols)}
		from "${KEYSPACE_PREFIX + stashName}".fs
		WHERE parent = ? AND basename = ?`,
		[parent, basename]
	);
	A.lte(result.rows.length, 1);
	if (!result.rows.length) {
		throw new NoSuchPathError(
			`No entry with parent=${parent.toString('hex')}` +
			` and basename=${inspect(basename)}`);
	}
	return result.rows[0];
}

async function getUuidForPath(client, stashName, p) {
	T(client, cassandra.Client, stashName, T.string, p, T.string);
	if (p === "") {
		// root directory is 0
		return Buffer.alloc(128 / 8);
	}

	const parentPath = utils.getParentPath(p);
	const parent     = await getUuidForPath(client, stashName, parentPath);
	const basename   = p.split("/").pop();

	const row = await getRowByParentBasename(client, stashName, parent, basename, ['type', 'uuid']);
	if (row.type !== "d") {
		throw new NoSuchPathError(`${inspect(p)} in ${stashName} is not a directory`);
	}
	T(row.uuid, Buffer);
	return row.uuid;
}

async function getRowByPath(client, stashName, p, cols) {
	T(client, cassandra.Client, stashName, T.string, p, T.string, cols, utils.ColsType);
	const parentPath = utils.getParentPath(p);
	const parent = await getUuidForPath(client, stashName, parentPath);
	const basename = p.split("/").pop();
	return getRowByParentBasename(client, stashName, parent, basename, cols);
}

function getChildrenForParent(client, stashName, parent, cols, limit) {
	T(client, cassandra.Client, stashName, T.string, parent, Buffer, cols, utils.ColsType, limit, T.optional(T.number));
	return new Promise(function getChildForParent$Promise(resolve, reject) {
		const rows = [];
		const rowStream = client.stream(
			`SELECT ${utils.colsAsString(cols)}
			from "${KEYSPACE_PREFIX + stashName}".fs
			WHERE parent = ?
			${limit === undefined ? "" : `LIMIT ${limit}`}`,
			[parent], {autoPage: true, prepare: true}
		);
		rowStream.once('error', reject);
		rowStream.on('readable', function getChildForParent$rowStream$readable() {
			let row;
			while (row = this.read()) {
				rows.push(row);
			}
		});
		rowStream.on('end', function getChildForParent$rowStream$end() {
			resolve(rows);
		});
	});
}

async function lsPath(client, stashName, options, p) {
	T(client, cassandra.Client, stashName, T.maybe(T.string), options, T.object, p, T.string);
	const stashInfo = await getStashInfoForNameOrPaths(stashName, [p]);
	const dbPath = eitherPathToDatabasePath(stashName, stashInfo.path, p);
	const parent = await getUuidForPath(client, stashInfo.name, dbPath);
	// If user wants just names and we're not sorting, we can put a little less load
	// on cassandra by getting just the `basename`s
	const justBasenames = options.justNames && !(options.sortByMtime || options.sortBySize);
	const rows = await getChildrenForParent(
		client, stashInfo.name, parent,
		justBasenames ? ["basename"] : ["basename", "type", "size", "mtime", "executable"]
	);
	if (options.sortByMtime) {
		rows.sort(options.reverse ? mtimeSorterAsc : mtimeSorterDesc);
	} else if (options.sortBySize) {
		rows.sort(options.reverse ? sizeSorterAsc : sizeSorterDesc);
	} else {
		rows.sort(options.reverse ? pathsorterDesc : pathsorterAsc);
	}
	for (const row of rows) {
		A(!/[\r\n]/.test(row.basename), `${inspect(row.basename)} contains CR or LF`);
		if (options.justNames) {
			console.log(row.basename);
		} else {
			let decoratedName = row.basename;
			if (row.type === 'd') {
				decoratedName = chalk.bold.blue(decoratedName);
				decoratedName += '/';
			} else if (row.executable) {
				decoratedName = chalk.bold.green(decoratedName);
				decoratedName += '*';
			}
			console.log(
				utils.pad(commaify((row.size || 0).toString()), 18) + " " +
				utils.shortISO(row.mtime) + " " +
				decoratedName
			);
		}
	}
}

async function listRecursively(client, stashInfo, baseDbPath, dbPath, print0, type) {
	T(client, cassandra.Client, stashInfo, T.object, dbPath, T.string, print0, T.boolean, type, T.optional(T.string));
	const parent = await getUuidForPath(client, stashInfo.name, dbPath);
	const rows = await getChildrenForParent(
		client, stashInfo.name, parent,
		["basename", "type"]
	);
	rows.sort(pathsorterAsc);
	for (const row of rows) {
		A(!/[\r\n]/.test(row.basename), `${inspect(row.basename)} contains CR or LF`);
		let fullPath = `${dbPath}/${row.basename}`;
		if (type === undefined || type === row.type) {
			const pathWithoutBase = fullPath.replace(baseDbPath + "/", "");
			process.stdout.write(pathWithoutBase + (print0 ? "\0" : "\n"));
		}
		if (row.type === "d") {
			await listRecursively(client, stashInfo, baseDbPath, fullPath, print0, type);
		}
	}
}

// Like "find" utility
function findPath(stashName, p, options) {
	T(stashName, T.maybe(T.string), p, T.string, options, T.object);
	return doWithClient(getNewClient(), function findPath$doWithClient(client) {
		return doWithPath(stashName, p, async function findPath$coro(stashInfo, dbPath, _parentPath) {
			await listRecursively(client, stashInfo, dbPath, dbPath, options.print0, options.type);
		});
	});
}

const MISSING   = Symbol('MISSING');
const DIRECTORY = Symbol('DIRECTORY');
const FILE      = Symbol('FILE');

async function getTypeInDbByParentBasename(client, stashName, parent, basename) {
	T(client, cassandra.Client, stashName, T.string, parent, Buffer, basename, T.string);
	let row;
	try {
		row = await getRowByParentBasename(client, stashName, parent, basename, ['type']);
	} catch(err) {
		if (!(err instanceof NoSuchPathError)) {
			throw err;
		}
		return MISSING;
	}
	if (row.type === "f") {
		return FILE;
	} else if (row.type === "d") {
		return DIRECTORY;
	} else {
		throw new Error(
			`Unexpected type in db for parent=${parent.toString('hex')}` +
			` basename=${inspect(basename)}: ${inspect(row.type)}`
		);
	}
};

async function getTypeInDbByPath(client, stashName, dbPath) {
	T(client, cassandra.Client, stashName, T.string, dbPath, T.string);
	if (dbPath === "") {
		// The root directory
		return DIRECTORY;
	}
	const parent = await getUuidForPath(client, stashName, utils.getParentPath(dbPath));
	return getTypeInDbByParentBasename(client, stashName, parent, utils.getBaseName(dbPath));
};

async function getTypeInWorkingDirectory(p) {
	T(p, T.string);
	try {
		const stat = await fs.statAsync(p);
		if (stat.isDirectory()) {
			return DIRECTORY;
		} else {
			return FILE;
		}
	} catch(err) {
		if (err.code !== 'ENOENT') {
			throw err;
		}
		return MISSING;
	}
};

class PathAlreadyExistsError extends Error {
	get name() {
		return this.constructor.name;
	}
}

const MIN_SUPPORTED_VERSION = 2;
const MAX_SUPPORTED_VERSION = 3;
const CURRENT_VERSION       = 3;

function checkDbPath(dbPath) {
	T(dbPath, T.string);
	if (!dbPath) {
		// Empty dbPath is OK; it means root directory
		return;
	}
	dbPath.split('/').map(filename.check);
}

async function makeDirsInDb(client, stashName, p, dbPath) {
	T(client, cassandra.Client, stashName, T.string, p, T.string, dbPath, T.string);
	checkDbPath(dbPath);
	// If directory does not exist on host fs, we'll use the current time.
	let mtime = utils.dateNow();
	try {
		mtime = (await fs.statAsync(p)).mtime;
	} catch(err) {
		if (err.code !== 'ENOENT') {
			throw err;
		}
	}
	const parentPath = utils.getParentPath(dbPath);
	if (parentPath) {
		await makeDirsInDb(client, stashName, p, parentPath);
	}
	const typeInDb = await getTypeInDbByPath(client, stashName, dbPath);
	if (typeInDb === MISSING) {
		const parentUuid = await getUuidForPath(client, stashName, utils.getParentPath(dbPath));
		const uuid = makeUuid();
		const added_time = utils.dateNow();
		await runQuery(
			client,
			`INSERT INTO "${KEYSPACE_PREFIX + stashName}".fs
			(basename, parent, uuid, type, mtime, version, added_time, added_user, added_host, added_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
			[utils.getBaseName(dbPath), parentUuid, uuid, 'd', mtime, CURRENT_VERSION, added_time, USERNAME, HOSTNAME, TERASTASH_VERSION]
		);
	} else if (typeInDb === FILE) {
		throw new PathAlreadyExistsError(
			`Cannot mkdir in database:` +
			` ${inspect(dbPath)} in stash ${inspect(stashName)}` +
			` already exists as a file`);
	} else if (typeInDb === DIRECTORY) {
		// do nothing
	}
};

async function tryCreateColumnOnStashTable(client, stashName, columnName, type) {
	T(client, cassandra.Client, stashName, T.string, columnName, T.string, type, T.string);
	try {
		await runQuery(client,
			`ALTER TABLE "${KEYSPACE_PREFIX + stashName}".fs ADD
			"${columnName}" ${type}`
		);
	} catch(err) {
		if (!(/^ResponseError: Invalid column name.*conflicts with an existing column$/.test(String(err)))) {
			throw err;
		}
	}
}

function makeKey() {
	if (Number(process.env.TERASTASH_INSECURE_AND_DETERMINISTIC)) {
		const keyCounter = new utils.PersistentCounter(
			path.join(process.env.TERASTASH_COUNTERS_DIR, 'file-key-counter'));
		const buf = Buffer.alloc(16);
		buf.writeIntBE(keyCounter.getNext(), 16 - 6 , 6);
		return buf;
	} else {
		return crypto.randomBytes(16);
	}
}

function makeUuid() {
	let uuid;
	if (Number(process.env.TERASTASH_INSECURE_AND_DETERMINISTIC)) {
		const uuidCounter = new utils.PersistentCounter(
			path.join(process.env.TERASTASH_COUNTERS_DIR, 'file-uuid-counter'), 1);
		const buf = Buffer.alloc(16);
		buf.writeIntBE(uuidCounter.getNext(), 16 - 6, 6);
		uuid = buf;
	} else {
		uuid = crypto.randomBytes(16);
	}
	A(
		!uuid.equals(Buffer.alloc(16)),
		"uuid must not be 0 because root directory is 0"
	);
	return uuid;
}

async function getChunkStore(stashInfo) {
	const storeName = stashInfo.chunkStore;
	if (!storeName) {
		throw new Error("stash info doesn't specify chunkStore key");
	}
	const config = await getChunkStores();
	const chunkStore = config.stores[storeName];
	if (!chunkStore) {
		throw new Error(`Chunk store ${storeName} is not defined in chunk-stores.json`);
	}
	chunkStore.name = storeName;
	return chunkStore;
}

async function dropFile(client, stashInfo, dbPath) {
	T(client, cassandra.Client, stashInfo, T.object, dbPath, T.string);
	const chunkStore = await getChunkStore(stashInfo);
	const parentUuid = await getUuidForPath(client, stashInfo.name, utils.getParentPath(dbPath));
	let chunks = null;
	try {
		const row = await getRowByParentBasename(
			client, stashInfo.name, parentUuid, utils.getBaseName(dbPath),
			[`chunks_in_${chunkStore.name}`]
		);
		chunks = row[`chunks_in_${chunkStore.name}`];
	} catch(err) {
		if (!isColumnMissingError(err)) {
			throw err;
		}
	}
	const row = await getRowByParentBasename(
		client, stashInfo.name, parentUuid, utils.getBaseName(dbPath),
		["type", "uuid"]
	);
	if (row.type === 'd') {
		const childRows = await getChildrenForParent(client, stashInfo.name, row.uuid, ["basename"], 1);
		if (childRows.length) {
			throw new DirectoryNotEmptyError(
				`Refusing to drop ${inspect(dbPath)} because it is a non-empty directory`
			);
		}
	}
	// TODO: Instead of DELETE, mark file with 'deleting' or something in case
	// the chunk-deletion process needs to be resumed later.
	await runQuery(
		client,
		`DELETE FROM "${KEYSPACE_PREFIX + stashInfo.name}".fs
		WHERE parent = ? AND basename = ?;`,
		[parentUuid, utils.getBaseName(dbPath)]
	);
	if (chunks !== null) {
		validateChunksFixBigints(chunks);
		if (chunkStore.type === "localfs") {
			await localfs.deleteChunks(chunkStore.directory, chunks);
		} else {
			const gdriver = new gdrive.GDriver(chunkStore.clientId, chunkStore.clientSecret);
			await gdrive.deleteChunks(gdriver, chunks);
		}
	}
}

/**
 * Remove files from the Cassandra database and their corresponding chunks.
 */
function dropFiles(stashName, paths) {
	T(stashName, T.maybe(T.string), paths, T.list(T.string));
	return doWithClient(getNewClient(), async function dropFiles$coro(client) {
		const stashInfo = await getStashInfoForNameOrPaths(stashName, paths);
		for (const p of paths) {
			const dbPath = eitherPathToDatabasePath(stashName, stashInfo.path, p);
			await dropFile(client, stashInfo, dbPath);
		}
	});
}

async function makeEmptySparseFile(p, size) {
	T(p, T.string, size, T.number);
	// First delete the existing file because it may have hard links, and we
	// don't want to overwrite the content of said hard links.
	await utils.tryUnlink(p);
	const handle = await fs.openAsync(p, "w");
	try {
		await fs.ftruncateAsync(handle, size);
	} finally {
		await fs.closeAsync(handle);
	}
}

async function makeFakeFile(p, size, mtime) {
	T(p, T.string, size, T.number, mtime, Date);
	await makeEmptySparseFile(p, size);
	await utils.utimesMilliseconds(p, mtime, mtime);
	// TODO: do this without a stat?
	const stat = await fs.statAsync(p);
	const withSticky = stat.mode | 0o1000;
	await fs.chmodAsync(p, withSticky);
}

async function infoFile(client, stashInfo, dbPath, showKeys) {
	T(client, cassandra.Client, stashInfo, StashInfoType, dbPath, T.string, showKeys, T.boolean);
	const row = await getRowByPath(client, stashInfo.name, dbPath, [utils.WILDCARD]);
	if (row.size !== null) {
		utils.assertSafeNonNegativeLong(row.size);
		row.size = Number(row.size);
	}
	for (const k of Object.keys(row)) {
		if (row[k] instanceof Buffer) {
			row[k] = row[k].toString('hex');
		}
		if (k.startsWith('chunks_in') && row[k]) {
			for (const chunkInfo of row[k]) {
				if (chunkInfo.crc32c) {
					chunkInfo.crc32c = chunkInfo.crc32c.toString('hex');
				}
				if (chunkInfo.md5) {
					chunkInfo.md5 = chunkInfo.md5.toString('hex');
				}
				utils.assertSafeNonNegativeLong(chunkInfo.size);
				chunkInfo.size = Number(chunkInfo.size);
			}
		}
	}
	if (!showKeys && row.key) {
		row.key = 'X'.repeat(row.key.length);
	}
	console.log(JSON.stringify(row, null, 2));
}

function infoFiles(stashName, paths, showKeys) {
	T(stashName, T.maybe(T.string), paths, T.list(T.string), showKeys, T.boolean);
	return doWithClient(getNewClient(), async function infoFiles$coro(client) {
		const stashInfo = await getStashInfoForNameOrPaths(stashName, paths);
		for (const p of paths) {
			const dbPath = eitherPathToDatabasePath(stashName, stashInfo.path, p);
			await infoFile(client, stashInfo, dbPath, showKeys);
		}
	});
}

async function shooFile(client, stashInfo, p, justRemove, ignoreMtime) {
	T(client, cassandra.Client, stashInfo, StashInfoType, p, T.string, justRemove, T.optional(T.boolean), ignoreMtime, T.optional(T.boolean));
	const dbPath = userPathToDatabasePath(stashInfo.path, p);
	const row = await getRowByPath(client, stashInfo.name, dbPath, ['mtime', 'size', 'type']);
	if (row.type === 'd') {
		throw new NotAFileError(`Can't shoo dbPath=${inspect(dbPath)}; it is a directory`);
	} else if (row.type === 'f') {
		const stat = await fs.statAsync(p);
		T(stat.mtime, Date);
		if (!ignoreMtime) {
			if (stat.mtime.getTime() !== Number(row.mtime)) {
				throw new UnexpectedFileError(
					`mtime for working directory file ${inspect(p)} is \n${stat.mtime.toISOString()}` +
					` but mtime for dbPath=${inspect(dbPath)} is` +
					`\n${new Date(Number(row.mtime)).toISOString()}`
				);
			}
		}
		T(stat.size, T.number);
		if (stat.size !== Number(row.size)) {
			throw new UnexpectedFileError(
				`size for working directory file ${inspect(p)} is \n${commaify(stat.size)}` +
				` but size for dbPath=${inspect(dbPath)} is \n${commaify(Number(row.size))}`
			);
		}
		if (justRemove) {
			await utils.tryUnlink(p);
		} else {
			await makeFakeFile(p, stat.size, row.mtime);
		}
	} else {
		throw new Error(`Unexpected type ${inspect(row.type)} for dbPath=${inspect(dbPath)}`);
	}
}

function shooFiles(paths, justRemove, continueOnError, ignoreMtime) {
	T(paths, T.list(T.string), justRemove, T.optional(T.boolean), continueOnError, T.optional(T.boolean), ignoreMtime, T.optional(T.boolean));
	return doWithClient(getNewClient(), async function shooFiles$coro(client) {
		const stashInfo = await getStashInfoForPaths(paths);
		for (const p of paths) {
			try {
				await shooFile(client, stashInfo, p, justRemove, ignoreMtime);
			} catch(err) {
				if (!(err instanceof UnexpectedFileError ||
					err instanceof NoSuchPathError)
				|| !continueOnError) {
					throw err;
				}
				console.error(chalk.red(err.message));
			}
		}
	});
}

const GCM_TAG_SIZE = 16;
// Does *not* include the length of the GCM tag itself
const DEFAULT_GCM_BLOCK_SIZE = (64 * 1024) - GCM_TAG_SIZE;

function checkChunkSize(size) {
	T(size, T.number);
	// (GCM block size + GCM tag length) must be a multiple of chunkSize, for
	// implementation convenience.
	if (size % (DEFAULT_GCM_BLOCK_SIZE + GCM_TAG_SIZE) !== 0) {
		throw new Error(`Chunk size must be a multiple of ` +
			`${DEFAULT_GCM_BLOCK_SIZE + GCM_TAG_SIZE}; got ${size}`);
	}
}

/**
 * Put file `p` into the Cassandra database as path `dbPath`.
 *
 * If `dropOldIfDifferent`, if the path in db already exists and the corresponding local
 * file has a different (mtime, size, executable), drop the db path and add the new file.
 */
async function addFile(outCtx, client, stashInfo, p, dbPath, dropOldIfDifferent=false, ignoreMtime=false) {
	T(
		client, cassandra.Client,
		stashInfo, StashInfoType,
		p, T.string,
		dbPath, T.string,
		dropOldIfDifferent, T.boolean,
		ignoreMtime, T.boolean
	);
	checkDbPath(dbPath);

	let oldRow;
	async function throwIfAlreadyInDb() {
		let caught = false;
		try {
			oldRow = await getRowByPath(client, stashInfo.name, dbPath,
				ignoreMtime ?
					['size', 'type', 'executable'] :
					['size', 'type', 'executable', 'mtime']);
		} catch(e) {
			if (!(e instanceof NoSuchPathError)) {
				throw e;
			}
			caught = true;
		}
		if (!caught) {
			throw new PathAlreadyExistsError(
				`Cannot add to database:` +
				` ${inspect(dbPath)} in stash ${inspect(stashInfo.name)}` +
				` already exists as a ${oldRow.type === 'd' ? "directory" : "file"}`);
		}
	}

	const stat = await fs.statAsync(p);
	if (!stat.isFile()) {
		throw new Error(`Cannot add ${inspect(p)} because it is not a file`);
	}
	const type       = 'f';
	const mtime      = stat.mtime;
	const executable = Boolean(stat.mode & 0o100); /* S_IXUSR */
	const sticky     = Boolean(stat.mode & 0o1000);
	if (sticky) {
		throw new UnexpectedFileError(
			`Refusing to add file ${inspect(p)} because it has sticky bit set,` +
			` which may have been set by 'ts shoo'`
		);
	}

	try {
		// Check early to avoid uploading to chunk store and doing other work
		await throwIfAlreadyInDb();
	} catch(e) {
		if (!(e instanceof PathAlreadyExistsError) || !dropOldIfDifferent) {
			throw e;
		}
		// User wants to replace old file in db, but only if new file is different
		const newFile = ignoreMtime ?
			{type: 'f', executable, size: stat.size} :
			{type: 'f', mtime, executable, size: stat.size};
		oldRow.size = Number(oldRow.size);
		//console.log({newFile, oldRow});
		if (!deepEqual(newFile, oldRow)) {
			const table = new Table({
				chars: {'mid': '', 'left-mid': '', 'mid-mid': '', 'right-mid': ''},
				head: ignoreMtime ?
					['which', 'size', 'executable'] :
					['which', 'mtime', 'size', 'executable']
			});
			if (ignoreMtime) {
				table.push(['old', commaify(oldRow.size), oldRow.executable]);
				table.push(['new', commaify(stat.size), executable]);
			} else {
				table.push(['old', String(oldRow.mtime), commaify(oldRow.size), oldRow.executable]);
				table.push(['new', String(mtime), commaify(stat.size), executable]);
			}
			console.log(`Notice: replacing ${inspect(dbPath)} in db\n${table.toString()}`);
			await dropFile(client, stashInfo, dbPath);
		} else {
			throw e;
		}
	}

	const chunkStore = await getChunkStore(stashInfo);
	const getParents = async function() {
		const chunkStore = await getChunkStore(stashInfo);
		return chunkStore.parents;
	};
	let content = null;
	let chunkInfo;
	let size;
	const version = CURRENT_VERSION;
	const uuid = makeUuid();
	// For file stored in chunk store, whole-file crc32c is not available.
	let crc32c = null;
	let key = null;
	let block_size = null;

	if (stat.size >= stashInfo.chunkThreshold || p.endsWith(".jpg")) {
		key = makeKey();
		block_size = DEFAULT_GCM_BLOCK_SIZE;


		checkChunkSize(chunkStore.chunkSize);

		const sizeOfTags = GCM_TAG_SIZE * Math.ceil(stat.size / block_size);
		const sizeWithTags = stat.size + sizeOfTags;
		utils.assertSafeNonNegativeInteger(sizeWithTags);

		const concealedSize = utils.concealSize(sizeWithTags);
		utils.assertSafeNonNegativeInteger(concealedSize);
		A.gte(concealedSize, sizeWithTags);

		// Note: a 1GB chunk has less than 1GB of data because of the GCM tags
		// every 8176 bytes.
		const dataBytesPerChunk = chunkStore.chunkSize * (block_size / (block_size + GCM_TAG_SIZE));
		utils.assertSafeNonNegativeInteger(dataBytesPerChunk);
		let startData = -dataBytesPerChunk;
		let startChunk = -chunkStore.chunkSize;

		// getChunkStream is like a next() on an iterator, except caller can pass
		// in `true` to get the last chunk again.  This "rewinding" is necessary
		// because upload of a chunk may fail and need to be retried.   We don't
		// want to re-read the entire file just to continue with the chunk we need
		// again.
		async function getChunkStream(lastChunkAgain) {
			T(lastChunkAgain, T.boolean);

			if (!lastChunkAgain) {
				startData += dataBytesPerChunk;
				startChunk += chunkStore.chunkSize;
			}
			utils.assertSafeNonNegativeInteger(startData);
			utils.assertSafeNonNegativeInteger(startChunk);

			if (startChunk >= concealedSize) {
				// No more chunk streams
				return null;
			}

			// Ensure that file is still the same size before opening it again
			const statAgain = await fs.statAsync(p);
			if (statAgain.size !== stat.size) {
				throw new FileChangedError(
					`Size of ${inspect(p)} changed from\n` +
					`${commaify(stat.size)} to\n${commaify(statAgain.size)}`
				);
			}
			if (statAgain.mtime.getTime() !== stat.mtime.getTime()) {
				throw new FileChangedError(
					`mtime of ${inspect(p)} changed from\n` +
					`${inspect(stat.mtime)} to\n${inspect(statAgain.mtime)}`
				);
			}

			// - 1 because byte range is inclusive on both start and end
			// This may get us a stream with 0 bytes because we've already
			// read everything.  We still need to continue because we may
			// not have finished streaming the padding.
			const inputStream = fs.createReadStream(
				p, {start: startData, end: (startData + dataBytesPerChunk - 1)});

			const iv = startData / block_size;
			//console.error("addFile", {iv: iv});
			utils.assertSafeNonNegativeInteger(iv);

			const cipherStream = new gcmer.GCMWriter(block_size, key, iv);
			utils.pipeWithErrors(inputStream, cipherStream);

			// Last chunk and need padding?
			const needPadding = startChunk + chunkStore.chunkSize > sizeWithTags;
			let outStream;
			if (needPadding) {
				outStream = new Combine();
				outStream.append(cipherStream);
				outStream.append(new random_stream.SecureRandomStream(concealedSize - sizeWithTags));
				outStream.append(null);
			} else {
				outStream = cipherStream;
			}

			return outStream;
		}

		let _;
		if (chunkStore.type === "localfs") {
			_ = await localfs.writeChunks(outCtx, chunkStore.directory, getChunkStream);
		} else if (chunkStore.type === "gdrive") {
			const gdriver = new gdrive.GDriver(chunkStore.clientId, chunkStore.clientSecret);
			// Pass getParents instead of parents, because it may change during an upload
			// by an external program that updates which team drive to use.
			_ = await gdrive.writeChunks(outCtx, gdriver, getParents, getChunkStream);
		} else {
			throw new Error(`Unknown chunk store type ${inspect(chunkStore.type)}`);
		}

		const totalSize = _[0];
		chunkInfo = _[1];
		for (const info of chunkInfo) {
			A.lte(info.size, chunkStore.chunkSize, `uploaded a too-big chunk:\n${inspect(info)}`);
		}
		A.eq(totalSize, concealedSize,
			`For ${inspect(dbPath)}, wrote to chunks\n` +
			`${commaify(totalSize)} bytes instead of the expected\n` +
			`${commaify(concealedSize)} (concealed) bytes`);
		T(chunkInfo, Array);

		size = stat.size;
	} else {
		content = await fs.readFileAsync(p);
		crc32c = hasher.crcToBuf(sse4_crc32.calculate(content));
		size = content.length;
		A.eq(size, stat.size,
			`For ${inspect(dbPath)}, read\n` +
			`${commaify(size)} bytes instead of the expected\n` +
			`${commaify(stat.size)} bytes; did file change during reading?`);
	}

	async function insert() {
		const parentPath = utils.getParentPath(dbPath);
		if (parentPath) {
			await makeDirsInDb(client, stashInfo.name, path.dirname(p), parentPath);
		}
		// TODO: make makeDirsInDb return uuid so that we don't have to get it again
		const parentUuid = await getUuidForPath(client, stashInfo.name, parentPath);
		const added_time = utils.dateNow();
		return await runQuery(
			client,
			`INSERT INTO "${KEYSPACE_PREFIX + stashInfo.name}".fs
			(basename, parent, type, content, key, "chunks_in_${chunkStore.name}", size,
			crc32c, mtime, executable, version, block_size, uuid, added_time, added_user, added_host, added_version)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
			[
				utils.getBaseName(dbPath), parentUuid, type, content, key, chunkInfo, size,
				crc32c, mtime, executable, version, block_size, uuid, added_time, USERNAME, HOSTNAME, TERASTASH_VERSION
			]
		);
	}

	// Check again to narrow the race condition
	await throwIfAlreadyInDb();
	try {
		await insert();
	} catch(err) {
		if (!isColumnMissingError(err)) {
			throw err;
		}
		await tryCreateColumnOnStashTable(
			client, stashInfo.name, `chunks_in_${chunkStore.name}`, 'list<frozen<chunk>>');
		await throwIfAlreadyInDb();
		await insert();
	}
}

/**
 * Put files or directories into the Cassandra database.
 */
function addFiles(outCtx, paths, continueOnExists=false, dropOldIfDifferent=false, thenShoo=false, justRemove=false, ignoreMtime=false) {
	T(
		outCtx,             OutputContextType,
		paths,              T.list(T.string),
		continueOnExists,   T.boolean,
		dropOldIfDifferent, T.boolean,
		thenShoo,           T.boolean,
		justRemove,         T.boolean,
		ignoreMtime,        T.boolean
	);
	return doWithClient(getNewClient(), async function addFiles$coro(client) {
		const stashInfo = await getStashInfoForPaths(paths);

		// Capture ctrl-c and don't exit until the entire upload is done because
		// we want to avoid leaving around unreferenced chunks in our chunk stores.
		// Not that we can always prevent that from happening, but it's nice to
		// avoid it.
		let stopNow = false;
		function stopSoon() {
			console.log('Got SIGINT.  Stopping after I add the current file.');
			stopNow = true;
		}
		process.on('SIGINT', stopSoon);
		try {
			let count = 1;
			for (const p of paths) {
				if (outCtx.mode === 'terminal') {
					utils.clearOrLF(process.stdout);
					process.stdout.write(`${count}/${paths.length}...`);
				}
				const dbPath = userPathToDatabasePath(stashInfo.path, p);
				let error = null;
				try {
					await addFile(outCtx, client, stashInfo, p, dbPath, dropOldIfDifferent, ignoreMtime);
				} catch(err) {
					if (!(err instanceof PathAlreadyExistsError ||
						err instanceof UnexpectedFileError /* was sticky */)
					|| !continueOnExists) {
						throw err;
					}
					error = err;
					console.error(chalk.red(err.message));
				}
				if (thenShoo && !error) {
					await shooFile(client, stashInfo, p, justRemove, /*ignoreMtime=*/false);
				}
				if (stopNow) {
					break;
				}
				count++;
			}
		} finally {
			process.removeListener('SIGINT', stopSoon);
		}
	});
}

function validateChunksFixBigints(chunks) {
	T(chunks, T.list(T.shape({size: T.object, idx: T.number})));
	let expectIdx = 0;
	for (const chunk of chunks) {
		utils.assertSafeNonNegativeLong(chunk.size);
		chunk.size = Number(chunk.size);
		A.eq(chunk.idx, expectIdx, "Bad chunk data from database");
		expectIdx += 1;
	}
}

/**
 * For an array of chunkInfos, return a [[start1, end1], ...]
 * Where start1 (inclusive) and end1 (exclusive) indicate
 * block-indexed chunk boundaries.
 */
function chunksToBlockRanges(chunks, blockSize) {
	T(chunks, utils.ChunksType, blockSize, T.number);
	utils.assertSafeNonNegativeInteger(blockSize);
	const blockRanges = [];
	let start = 0;
	for (const c of chunks) {
		// Last chunk might not be divisible by blockSize
		const scaledSize = Math.ceil(c.size / blockSize);
		utils.assertSafeNonNegativeInteger(scaledSize);
		blockRanges.push([start, start + scaledSize]);
		start += scaledSize;
	}
	return blockRanges;
}

/**
 * Get a readable stream with the file contents, whether the file is in the db
 * or in a chunk store.
 */
async function streamFile(client, stashInfo, parent, basename, ranges) {
	T(client, cassandra.Client, stashInfo, T.object, parent, Buffer, basename, T.string, ranges, T.optional(T.list(utils.RangeType)));
	if (ranges) {
		A.eq(ranges.length, 1, "Only support 1 range right now");
		utils.assertSafeNonNegativeInteger(ranges[0][0]);
		utils.assertSafeNonNegativeInteger(ranges[0][1]);
	}
	// TODO: instead of checking just this one stash, check all stashes
	const storeName = stashInfo.chunkStore;
	if (!storeName) {
		throw new Error("stash info doesn't specify chunkStore key");
	}

	let row;
	try {
		row = await getRowByParentBasename(client, stashInfo.name, parent, basename,
			["size", "type", "key", `chunks_in_${storeName}`, "crc32c", "content", "mtime", "executable", "version", "block_size"]
		);
	} catch(err) {
		if (!isColumnMissingError(err)) {
			throw err;
		}
		// chunks_in_${storeName} doesn't exist, try the query without it
		row = await getRowByParentBasename(client, stashInfo.name, parent, basename,
			["size", "type", "key", "crc32c", "content", "mtime", "executable", "version", "block_size"]
		);
	}

	function describe() {
		return `parent=${parent.toString('hex')} basename=${inspect(basename)}`;
	}

	if (row.type !== 'f') {
		throw new NotAFileError(
			`Object ${describe()} in stash ${inspect(stashInfo.name)} is not a file; got type ${inspect(row.type)}`);
	}

	utils.assertSafeNonNegativeInteger(row.version);
	if (row.version < MIN_SUPPORTED_VERSION) {
		throw new Error(`File ${describe()} has version ${row.version}; ` +
			`min supported version is ${MIN_SUPPORTED_VERSION}.`);
	}
	if (row.version > MAX_SUPPORTED_VERSION) {
		throw new Error(`File ${describe()} has version ${row.version}; ` +
			`max supported version is ${MAX_SUPPORTED_VERSION}.`);
	}

	const chunkStore = (await getChunkStores()).stores[storeName];
	const chunks = row[`chunks_in_${storeName}`] || null;
	let bytesRead = 0;
	let dataStream;
	if (chunks !== null) {
		validateChunksFixBigints(chunks);
		A.eq(row.content, null);
		A.eq(row.key.length, 128 / 8);
		utils.assertSafeNonNegativeInteger(row.block_size);
		// To save CPU time, we check whole-chunk CRC32C's only for chunks
		// that don't have embedded authentication tags.
		const checkWholeChunkCRC32C = (row.block_size === 0);

		let wantedChunks;
		let wantedRanges;
		let truncateLeft;
		let truncateRight;
		let scaledRequestedRange;
		let returnedDataRange;
		// User requested a range, so we have to determine which chunks we actually
		// need to read and also carefully map the range to read on AES-128-CTR
		// or AES-128-GCM boundaries.
		if (ranges) {
			wantedChunks = [];
			wantedRanges = [];
			// block_size > 0 uses AES-128-GCM, block size == 0 uses AES-128-CTR with no
			// authentication tags or in-stream checksums.
			//
			// For AES-128-CTR, we'll unnecessarily read from the chunk store up to 15
			// bytes before and 15 bytes ahead, but that's totally okay.
			// For AES-128-GCM, we *have* to "unnecessarily" read up to block_size-1
			// bytes before and ahead because we must verify the GCM tags.
			const encodedBlockSize =
				row.block_size > 0 ?
					(row.block_size + GCM_TAG_SIZE) :
					aes.BLOCK_SIZE;
			// The actual amount of data we'll get per block after decryption.
			const decodedBlockSize =
				row.block_size > 0 ?
					row.block_size :
					aes.BLOCK_SIZE;
			const scaledChunkRanges = chunksToBlockRanges(chunks, encodedBlockSize);
			scaledRequestedRange = [
				Math.floor(ranges[0][0] / decodedBlockSize),
				Math.ceil(ranges[0][1] / decodedBlockSize)];
			let blocksSeen = 0;
			scaledChunkRanges.forEach(function(scaledChunkRange, idx) {
				const intersection = utils.intersect(scaledChunkRange, scaledRequestedRange);
				if (intersection !== null) {
					wantedChunks.push(chunks[idx]);
					// (- blockSeen) because we need to scale numbers back to ranges
					// relative to the start of each chunk.
					wantedRanges.push([
						(intersection[0] - blocksSeen) * encodedBlockSize,
						// Don't exceed the actual size of the last chunk, else we'll
						// produce a Range: request that has more than we can get back.
						Math.min((intersection[1] - blocksSeen) * encodedBlockSize, chunks[idx].size)]);
				}
				let blocksInChunk = chunks[idx].size / encodedBlockSize;
				// Last chunk might not be divisible by encodedBlockSize
				if (idx === scaledChunkRanges.length - 1) {
					blocksInChunk = Math.ceil(blocksInChunk);
				}
				utils.assertSafeNonNegativeInteger(blocksInChunk);
				blocksSeen += blocksInChunk;
			});
			returnedDataRange = [
				scaledRequestedRange[0] * decodedBlockSize,
				scaledRequestedRange[1] * decodedBlockSize];
			truncateLeft = ranges[0][0] - returnedDataRange[0];
			A.gte(truncateLeft, 0);
			// left-truncation will already have happened before right-truncation, so
			// here we just need to specify the length of the data we want.
			truncateRight = ranges[0][1] - ranges[0][0];
			A.gte(truncateRight, 0);
			//console.error({encodedBlockSize, decodedBlockSize, /*scaledChunkRanges,*/
			//	scaledRequestedRange, returnedDataRange, truncateLeft, truncateRight,
			//	wantedChunks, wantedRanges});
		} else {
			wantedChunks         = chunks;
			wantedRanges         = chunks.map(chunk => [0, chunk.size]);
			truncateLeft         = null;
			truncateRight        = null;
			scaledRequestedRange = [0, null];
			returnedDataRange    = [0, Number(row.size)];
		}
		A.eq(wantedChunks.length, wantedRanges.length);

		let cipherStream;
		if (chunkStore.type === "localfs") {
			const chunksDir = chunkStore.directory;
			cipherStream = localfs.readChunks(chunksDir, wantedChunks, wantedRanges, checkWholeChunkCRC32C);
		} else if (chunkStore.type === "gdrive") {
			const gdriver = new gdrive.GDriver(chunkStore.clientId, chunkStore.clientSecret);
			cipherStream = gdrive.readChunks(gdriver, wantedChunks, wantedRanges, checkWholeChunkCRC32C);
		} else {
			throw new Error(`Unknown chunk store type ${inspect(chunkStore.type)}`);
		}

		// We need to make sure we don't try to GCM-decrypt any of the padding.
		// If we go even one byte over, GCMReader will incorrectly assume that byte
		// is part of the last block, and authentication will fail.
		const sizeWithoutLeading = Number(row.size) - returnedDataRange[0];
		utils.assertSafeNonNegativeInteger(sizeWithoutLeading);
		if (row.block_size > 0) {
			const sizeOfTags   = GCM_TAG_SIZE * Math.ceil(sizeWithoutLeading / row.block_size);
			const sizeWithTags = sizeWithoutLeading + sizeOfTags;
			utils.assertSafeNonNegativeInteger(sizeWithTags);

			const unpaddedStream = new padded_stream.RightTruncate(sizeWithTags);
			utils.pipeWithErrors(cipherStream, unpaddedStream);

			dataStream = new gcmer.GCMReader(row.block_size, row.key, scaledRequestedRange[0]);
			utils.pipeWithErrors(unpaddedStream, dataStream);
		} else {
			const unpaddedStream = new padded_stream.RightTruncate(sizeWithoutLeading);
			utils.pipeWithErrors(cipherStream, unpaddedStream);

			dataStream = crypto.createCipheriv(
				'aes-128-ctr', row.key, aes.blockNumberToIv(scaledRequestedRange[0]));
			utils.pipeWithErrors(unpaddedStream, dataStream);
		}
		// Warning: dataStream may be rebound right below

		if (truncateLeft !== null) {
			const _dataStream = dataStream;
			dataStream = new padded_stream.LeftTruncate(truncateLeft);
			utils.pipeWithErrors(_dataStream, dataStream);
		}
		if (truncateRight !== null) {
			const _dataStream = dataStream;
			dataStream = new padded_stream.RightTruncate(truncateRight);
			utils.pipeWithErrors(_dataStream, dataStream);
		}

		dataStream.on('data', function(data) {
			bytesRead += data.length;
		});
		// We attached a 'data' handler, but don't let that put us into
		// flowing mode yet, because the user hasn't attached their own
		// 'data' handler yet.
		dataStream.pause();

		dataStream.destroy = function dataStream$destroy() {
			cipherStream.destroy();
		};
	} else {
		const content =
			ranges ?
				row.content.slice(ranges[0][0], ranges[0][1]) :
				row.content;
		dataStream = streamifier.createReadStream(content);
		dataStream.destroy = () => {};
		bytesRead = content.length;
		const crc32c = hasher.crcToBuf(sse4_crc32.calculate(row.content));
		// Note: only in-db content has a crc32c for entire file content
		if (!crc32c.equals(row.crc32c)) {
			dataStream.emit('error', new Error(
				`For ${describe()}, CRC32C is allegedly\n` +
				`${row.crc32c.toString('hex')} but CRC32C of data is\n` +
				`${crc32c.toString('hex')}`
			));
		}
	}
	dataStream.once('end', function streamFile$end() {
		const expectedBytesRead =
			ranges ?
				ranges[0][1] - ranges[0][0] :
				Number(row.size);
		if (bytesRead !== expectedBytesRead) {
			dataStream.emit('error', new Error(
				`For ${describe()}, expected length of content to be\n` +
				`${commaify(expectedBytesRead)} but was\n` +
				`${commaify(bytesRead)}`
			));
		}
	});
	return [row, dataStream];
}

/**
 * Get a file or directory from the Cassandra database.
 */
async function getFile(client, stashInfo, dbPath, outputFilename, fake, skipIfExists) {
	T(client, cassandra.Client, stashInfo, T.object, dbPath, T.string, outputFilename, T.string, fake, T.boolean, skipIfExists, T.boolean);
	await utils.mkdirpAsync(path.dirname(outputFilename));

	const row = await getRowByPath(client, stashInfo.name, dbPath, ['size', 'mtime']);

	let stat;
	try {
		stat = await fs.statAsync(outputFilename);
	} catch(err) {
		if (err.code !== "ENOENT") {
			throw err;
		}
		// file doesn't already exist locally, continue
	} finally {
		if (stat && stat.isFile() && stat.size === Number(row.size) && stat.mtime.getTime() === row.mtime.getTime()) {
			console.log(`Notice: skipping ${inspect(dbPath)} because it already exists locally with same size and mtime`);
			return;
		}
	}

	// Delete the existing file because it may
	// 1) have hard links
	// 2) have the sticky bit set
	// 3) have other unwanted permissions set
	await utils.tryUnlink(outputFilename);

	if (fake) {
		await makeFakeFile(outputFilename, Number(row.size), row.mtime);
	} else {
		const parent = await getUuidForPath(client, stashInfo.name, utils.getParentPath(dbPath));
		const [_, readStream] = await streamFile(client, stashInfo, parent, utils.getBaseName(dbPath));
		const writeStream = fs.createWriteStream(outputFilename);
		utils.pipeWithErrors(readStream, writeStream);
		await new Promise(function getFiles$Promise(resolve, reject) {
			writeStream.once('finish', function getFile$writeStream$finish() {
				resolve();
			});
			writeStream.once('error', function getFile$writeStream$error(err) {
				reject(err);
			});
			readStream.once('error', function getFile$readStream$error(err) {
				reject(err);
			});
		});
		await utils.utimesMilliseconds(outputFilename, row.mtime, row.mtime);
		if (row.executable) {
			// TODO: setting for 0o700 instead?
			await fs.chmodAsync(outputFilename, 0o770);
		}
	}
}

function getFiles(stashName, paths, fake, skipIfExists) {
	T(stashName, T.maybe(T.string), paths, T.list(T.string), fake, T.boolean, skipIfExists, T.boolean);
	return doWithClient(getNewClient(), async function getFiles$coro(client) {
		const stashInfo = await getStashInfoForNameOrPaths(stashName, paths);
		for (const p of paths) {
			let dbPath;
			let outputFilename;
			// If stashName was given, write file to current directory
			if (stashName) {
				dbPath = p;
				outputFilename = p;
			} else {
				dbPath = userPathToDatabasePath(stashInfo.path, p);
				outputFilename = stashInfo.path + '/' + dbPath;
			}

			await getFile(client, stashInfo, dbPath, outputFilename, fake, skipIfExists);
		}
	});
}

async function catFile(client, stashInfo, dbPath, ranges) {
	T(client, cassandra.Client, stashInfo, T.object, dbPath, T.string, ranges, T.optional(T.list(utils.RangeType)));
	const parent = await getUuidForPath(client, stashInfo.name, utils.getParentPath(dbPath));
	const [_row, readStream] = await streamFile(client, stashInfo, parent, utils.getBaseName(dbPath), ranges);
	const p = new Promise(function(resolve, reject) {
		readStream.on('end', resolve);
		readStream.once('error', reject);
	});
	utils.pipeWithErrors(readStream, process.stdout);
	await p;
}

function catFiles(stashName, paths) {
	T(stashName, T.maybe(T.string), paths, T.list(T.string));
	return doWithClient(getNewClient(), async function catFiles$coro(client) {
		const stashInfo = await getStashInfoForNameOrPaths(stashName, paths);
		for (const p of paths) {
			const dbPath = eitherPathToDatabasePath(stashName, stashInfo.path, p);
			await catFile(client, stashInfo, dbPath);
		}
	});
}

function catRangedFiles(stashName, args) {
	T(stashName, T.maybe(T.string), args, T.list(T.string));
	return doWithClient(getNewClient(), async function catFiles$coro(client) {
		const paths = args.map(function(s) { return utils.rsplitString(s, "/", 1)[0]; });
		const stashInfo = await getStashInfoForNameOrPaths(stashName, paths);
		for (const a of args) {
			const [p, range] = utils.rsplitString(a, "/", 1);
			let [start, end] = utils.splitString(range, "-", 1);
			start = Number(start);
			end = Number(end);
			utils.assertSafeNonNegativeInteger(start);
			utils.assertSafeNonNegativeInteger(end);
			const ranges = [[start, end]];
			const dbPath = eitherPathToDatabasePath(stashName, stashInfo.path, p);
			await catFile(client, stashInfo, dbPath, ranges);
		}
	});
}

function makeDirectories(stashName, paths) {
	T(stashName, T.maybe(T.string), paths, T.list(T.string));
	return doWithClient(getNewClient(), async function makeDirectories$coro(client) {
		let dbPaths;
		let stashInfo;
		if (stashName) { // Explicit stash name provided
			stashInfo = await getStashInfoByName(stashName);
			dbPaths = paths;
		} else {
			stashInfo = await getStashInfoForPaths(paths);
			dbPaths = paths.map(function(p) {
				return userPathToDatabasePath(stashInfo.path, p);
			});
		}
		for (let i=0; i < dbPaths.length; i++) {
			const p = paths[i];
			const dbPath = dbPaths[i];
			checkDbPath(dbPath);
			try {
				await utils.mkdirpAsync(p);
			} catch(err) {
				if (err.code !== 'EEXIST') {
					throw err;
				}
				throw new PathAlreadyExistsError(
					`Cannot mkdir in working directory:` +
					` ${inspect(p)} already exists and is not a directory`
				);
			}
			await makeDirsInDb(client, stashInfo.name, p, dbPath);
		}
	});
}

function moveFiles(stashName, sources, dest) {
	T(stashName, T.maybe(T.string), sources, T.list(T.string), dest, T.string);
	return doWithClient(getNewClient(), async function moveFiles$coro(client) {
		let stashInfo;
		let dbPathSources;
		let dbPathDest;
		if (stashName) { // Explicit stash name provided
			stashInfo = await getStashInfoByName(stashName);
			dbPathSources = sources;
			dbPathDest = dest;
		} else {
			stashInfo = await getStashInfoForPaths(sources.concat(dest));
			dbPathSources = sources.map(function(p) {
				return userPathToDatabasePath(stashInfo.path, p);
			});
			dbPathDest = userPathToDatabasePath(stashInfo.path, dest);
		}
		checkDbPath(dbPathDest);

		// This is inherently racy; type may be different by the time we mv
		let destTypeInDb = await getTypeInDbByPath(client, stashInfo.name, dbPathDest);
		// TODO XXX: is this right? what about when -n is specified?
		const destInWorkDir = path.join(stashInfo.path, dbPathDest);
		const destTypeInWorkDir = await getTypeInWorkingDirectory(destInWorkDir);

		if (destTypeInDb === MISSING && destTypeInWorkDir === DIRECTORY) {
			await makeDirsInDb(client, stashInfo.name, dest, dbPathDest);
			destTypeInDb = DIRECTORY;
		}

		if (destTypeInDb === FILE) {
			throw new PathAlreadyExistsError(
				`Cannot mv in database: destination ${inspect(dbPathDest)}` +
				` already exists in stash ${inspect(stashInfo.name)}`
			);
		}
		if (destTypeInWorkDir === FILE) {
			throw new PathAlreadyExistsError(
				`Cannot mv in working directory: refusing to overwrite ${inspect(dest)}` +
				` in working directory`
			);
		}
		if (destTypeInDb === DIRECTORY) {
			for (const dbPathSource of dbPathSources) {
				const parent = await getUuidForPath(
					client, stashInfo.name, utils.getParentPath(dbPathSource));
				const row = await getRowByParentBasename(
					client, stashInfo.name, parent, utils.getBaseName(dbPathSource), [utils.WILDCARD]);
				row.parent = await getUuidForPath(client, stashInfo.name, dbPathDest);
				// row.basename is unchanged
				const cols = Object.keys(row);
				const qMarks = utils.filledArray(cols.length, "?");

				// This one checks the actual dir/basename instead of the dir/
				let actualDestTypeInDb = await getTypeInDbByParentBasename(
					client, stashInfo.name, row.parent, row.basename);
				if (actualDestTypeInDb !== MISSING) {
					throw new PathAlreadyExistsError(
						`Cannot mv in database: destination parent=${row.parent.toString('hex')}` +
						` basename=${inspect(row.basename)} already exists in stash ${inspect(stashInfo.name)}`
					);
				}

				const actualDestInWorkDir = path.join(
					stashInfo.path, dbPathDest, utils.getBaseName(dbPathSource));
				const actualDestTypeInWorkDir = await getTypeInWorkingDirectory(actualDestInWorkDir);
				if (actualDestTypeInWorkDir !== MISSING) {
					throw new PathAlreadyExistsError(
						`Cannot mv in working directory: refusing to overwrite` +
						` ${inspect(actualDestInWorkDir)}`
					);
				}

				await runQuery(
					client,
					`INSERT INTO "${KEYSPACE_PREFIX + stashInfo.name}".fs
					(${utils.colsAsString(cols)})
					VALUES (${qMarks.join(", ")});`,
					cols.map(function(col) { return row[col]; })
				);

				await runQuery(
					client,
					`DELETE FROM "${KEYSPACE_PREFIX + stashInfo.name}".fs
					WHERE parent = ? AND basename = ?;`,
					[parent, utils.getBaseName(dbPathSource)]
				);

				// Now move the file in the working directory
				await utils.mkdirpAsync(path.dirname(actualDestInWorkDir));
				const srcInWorkDir = path.join(stashInfo.path, dbPathSource);
				try {
					await fs.renameAsync(srcInWorkDir, actualDestInWorkDir);
				} catch(err) {
					if (err.code !== "ENOENT") {
						throw err;
					}
					// It's okay if the file was missing in work dir
				}
			}
		} else {
			throw new Error("Haven't implemented mv to a non-dir dest yet");
		}

		/*else if (destTypeInDb === MISSING) {
			if (dbPathSources.length > 1) {

			}
		}*/
		//console.log({dbPathSources, dbPathDest, destTypeInDb});
	});
}

/**
 * List all terastash keyspaces in Cassandra
 */
function listTerastashKeyspaces() {
	return doWithClient(getNewClient(), function listTerastashKeyspaces$doWithClient(client) {
		// TODO: also display durable_writes, strategy_class, strategy_options  info in table
		return runQuery(
			client,
			`SELECT keyspace_name FROM System.schema_keyspaces;`
		).then(function listTerastashKeyspaces$then(result) {
			for (const row of result.rows) {
				const name = row.keyspace_name;
				if (name.startsWith(KEYSPACE_PREFIX)) {
					console.log(name.replace(KEYSPACE_PREFIX, ""));
				}
			}
		});
	});
}

async function listChunkStores() {
	const config = await getChunkStores();
	for (const storeName of Object.keys(config.stores)) {
		console.log(storeName);
	}
}

async function defineChunkStore(storeName, opts) {
	T(storeName, T.string, opts, T.object);
	const config = await getChunkStores();
	if (config.stores[storeName]) {
		throw new Error(`${storeName} is already defined in chunk-stores.json`);
	}
	if (typeof opts.chunkSize !== "number") {
		throw new UsageError(`.chunkSize is missing or not a number on ${inspect(opts)}`);
	}
	const storeDef = {type: opts.type, chunkSize: opts.chunkSize};
	if (opts.type === "localfs") {
		if (typeof opts.directory !== "string") {
			throw new UsageError(`Chunk store type localfs requires a -d/--directory ` +
				`parameter with a string; got ${opts.directory}`
			);
		}
		storeDef.directory = opts.directory;
	} else if (opts.type === "gdrive") {
		if (typeof opts.clientId !== "string") {
			throw new UsageError(`Chunk store type gdrive requires a --client-id ` +
				`parameter with a string; got ${opts.clientId}`
			);
		}
		storeDef.clientId = opts.clientId;
		if (typeof opts.clientSecret !== "string") {
			throw new UsageError(`Chunk store type gdrive requires a --client-secret ` +
				`parameter with a string; got ${opts.clientSecret}`
			);
		}
		storeDef.clientSecret = opts.clientSecret;
	} else {
		throw new UsageError(`Type must be "localfs" or "gdrive" but was ${opts.type}`);
	}
	config.stores[storeName] = storeDef;
	await utils.writeObjectToConfigFile("chunk-stores.json", config);
}

async function configChunkStore(storeName, opts) {
	T(storeName, T.string, opts, T.object);
	const config = await getChunkStores();
	if (!config.stores[storeName]) {
		throw new Error(`${storeName} is not defined in chunk-stores.json`);
	}
	if (opts.type !== undefined) {
		T(opts.type, T.string);
		config.stores[storeName].type = opts.type;
	}
	if (opts.chunkSize !== undefined) {
		T(opts.chunkSize, T.number);
		config.stores[storeName].chunkSize = opts.chunkSize;
	}
	if (opts.directory !== undefined) {
		T(opts.directory, T.string);
		config.stores[storeName].directory = opts.directory;
	}
	if (opts.clientId !== undefined) {
		T(opts.clientId, T.string);
		config.stores[storeName].clientId = opts.clientId;
	}
	if (opts.clientSecret !== undefined) {
		T(opts.clientSecret, T.string);
		config.stores[storeName].clientSecret = opts.clientSecret;
	}
	await utils.writeObjectToConfigFile("chunk-stores.json", config);
}

function questionAsync(question) {
	T(question, T.string);
	return new Promise(function questionAsync$Promise(resolve) {
		const rl = readline.createInterface({
			input:  process.stdin,
			output: process.stdout
		});
		rl.question(question, function(answer) {
			rl.close();
			resolve(answer);
		});
	});
}

async function authorizeGDrive(name) {
	T(name, T.string);
	const config = await getChunkStores();
	const stores = config.stores;
	if (!(typeof stores === "object" && stores !== null)) {
		throw new Error(`'stores' in chunk-stores.json is not an object`);
	}
	const chunkStore = stores[name];
	if (!(typeof chunkStore === "object" && chunkStore !== null)) {
		throw new Error(`Chunk store ${name} was ${chunkStore}, should be an object`);
	}
	if (!chunkStore.clientId) {
		throw new Error(`Chunk store ${name} is missing a clientId`);
	}
	if (!chunkStore.clientSecret) {
		throw new Error(`Chunk store ${name} is missing a clientSecret`);
	}
	const gdriver = new gdrive.GDriver(chunkStore.clientId, chunkStore.clientSecret);
	const url = gdriver.getAuthUrl();
	console.log("Please open this URL in a browser (one where" +
		" you are signed in to Google) and authorize the application:");
	console.log("");
	console.log(url);
	console.log("");
	console.log("Then, copy the authorization code from the input box, paste it here, and press Enter:");
	const authCode = await questionAsync("Authorization code: ");
	console.log("OK, sending the authorization code to Google to get a refresh token...");
	const account = await questionAsync("Email address of Google account that you used: ");
	await gdriver.importAuthCode(authCode, account);
	console.log("OK, saved the refresh token and access token.");
}

function atomicWriteFileSync(fname, content, tempDirectory) {
	const tempPath = `${tempDirectory}/.${path.basename(fname)}-${Math.random()}`;
	fs.writeFileSync(tempPath, content);
	fs.renameSync(tempPath, fname);
}

async function updateGoogleTokens(tokensFilename, clientId, clientSecret) {
	T(tokensFilename, T.string, clientId, T.string, clientSecret, T.string);
	const gdriver     = new gdrive.GDriver(clientId, clientSecret);
	const credentials = JSON.parse(fs.readFileSync(tokensFilename)).credentials[clientId];
	gdriver._oauth2Client.setCredentials(credentials);
	await gdriver.refreshAccessToken();

	// Write the file atomically to avoid the occasional
	// `SyntaxError: Unexpected end of JSON input` while the file is still being written.
	const tempDirectory = `${path.dirname(path.dirname(tokensFilename))}/temp`;
	await utils.mkdirpAsync(tempDirectory);
	const content = JSON.stringify({credentials: {[clientId]: gdriver._oauth2Client.credentials}}, null, 2);
	atomicWriteFileSync(tokensFilename, content, tempDirectory);
}

async function createTeamDrive(stashName, managerEmail, contentManagerListFile, driveName, teamDriveFile) {
	T(stashName, T.string, managerEmail, T.string, contentManagerListFile, T.string, driveName, T.string, teamDriveFile, T.string);
	const contentManagersText = fs.readFileSync(contentManagerListFile).toString("utf-8").trim();
	let contentManagers = [];
	if (contentManagersText !== "") {
		contentManagers = contentManagersText.split("\n");
	}
	const stashInfo  = await getStashInfoByName(stashName);
	const chunkStore = await getChunkStore(stashInfo);
	const gdriver    = new gdrive.GDriver(chunkStore.clientId, chunkStore.clientSecret);
	const id         = await gdriver.createTeamDrive(managerEmail, contentManagers, driveName);
	fs.appendFileSync(teamDriveFile, `${driveName}\t${id}\n`);
}

function assertName(name) {
	T(name, T.string);
	A(name, "Name must not be empty");
}

async function destroyStash(stashName) {
	assertName(stashName);
	await doWithClient(getNewClient(), async function destroyStash$doWithClient(client) {
		await runQuery(client, `DROP TABLE IF EXISTS "${KEYSPACE_PREFIX + stashName}".fs;`);
		await runQuery(client, `DROP TYPE IF EXISTS  "${KEYSPACE_PREFIX + stashName}".chunk;`);
		await runQuery(client, `DROP KEYSPACE        "${KEYSPACE_PREFIX + stashName}";`);
	});
	const config = await getStashes();
	delete config.stashes[stashName];
	await utils.writeObjectToConfigFile("stashes.json", config);
	console.log(`Destroyed keyspace and removed config for ${stashName}.`);
}

/**
 * Initialize a new stash
 */
async function initStash(stashPath, stashName, options) {
	T(
		stashPath, T.string,
		stashName, T.string,
		options,   T.shape({
			chunkStore:     T.string,
			chunkThreshold: T.number
		})
	);
	assertName(stashName);

	let caught;
	try {
		await getStashInfoByPath(stashPath);
	} catch(err) {
		if (!(err instanceof NotInWorkingDirectoryError)) {
			throw err;
		}
		caught = true;
	}
	if (!caught) {
		throw new Error(`${stashPath} is already configured as a stash`);
	}

	return doWithClient(getNewClient(), async function initStash$coro(client) {
		await runQuery(client, `CREATE KEYSPACE IF NOT EXISTS "${KEYSPACE_PREFIX + stashName}"
			WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`);

		// An individual chunk
		await runQuery(client, `CREATE TYPE "${KEYSPACE_PREFIX + stashName}".chunk (
			idx     int,
			file_id text,
			md5     blob,
			crc32c  blob,
			size    bigint,
			account text
		)`);

		await runQuery(client, `CREATE TABLE IF NOT EXISTS "${KEYSPACE_PREFIX + stashName}".fs (
			basename      text,
			type          ascii,
			parent        blob,
			uuid          blob,
			size          bigint,
			content       blob,
			crc32c        blob,
			key           blob,
			mtime         timestamp,
			executable    boolean,
			version       int,
			block_size    int,
			added_time    timestamp,
			added_user    text,
			added_host    text,
			added_version text,
			PRIMARY KEY (parent, basename)
		);`);
		// The above PRIMARY KEY lets us select on both parent and (parent, basename)

		// Note: chunks_in_* columns are added by tryCreateColumnOnStashTable.
		// We use column-per-chunk-store instead of having a map of
		// <chunkStore, chunkInfo> because non-frozen, nested collections
		// aren't implemented: https://issues.apache.org/jira/browse/CASSANDRA-7826

		const config = await getStashes();
		config.stashes[stashName] = {
			path:           path.resolve(stashPath),
			chunkStore:     options.chunkStore,
			chunkThreshold: options.chunkThreshold
		};
		await utils.writeObjectToConfigFile("stashes.json", config);
	});
}

let transitWriter;
function getTransitWriter() {
	if (!transitWriter) {
		transitWriter = transit.writer("json-verbose", {
			/* Don't need a cache because we're using json-verbose */
			cache: false,
			handlers: transit.map([
				cassandra.types.Row,
				transit.makeWriteHandler({
					tag: () => "Row",
					rep: v  => Object.assign({}, v)
				}),
				cassandra.types.Long,
				transit.makeWriteHandler({
					tag: () => "Long",
					rep: v  => String(v)
				})
			])
		});
		A.eq(transitWriter.cache, null);
	}
	return transitWriter;
}

let transitReader;
function getTransitReader() {
	if (!transitReader) {
		transitReader = transit.reader("json-verbose", {handlers: {
			"Long": v => {
				const long = cassandra.types.Long.fromString(v);
				A.eq(long.toString(), v);
				return long;
			},
			"Row": v => v
		}});
	}
	return transitReader;
}

class RowToTransit extends Transform {
	constructor() {
		super({objectMode: true});
		this.transitWriter = getTransitWriter();
	}

	_transform(row, encoding, callback) {
		try {
			this.push(this.transitWriter.write(row));
			this.push("\n");
		} catch(err) {
			callback(err);
			return;
		}
		callback();
	}
}

function exportDb(stashName) {
	T(stashName, T.maybe(T.string));
	return doWithClient(getNewClient(), function exportDb$doWithClient(client) {
		return doWithPath(stashName, ".", function exportDb$Promise(stashInfo, _dbPath, _parentPath) {
			T(stashInfo.name, T.string);
			return new Promise(function(resolve, reject) {
				const rowStream = client.stream(
					`SELECT * FROM "${KEYSPACE_PREFIX + stashInfo.name}".fs;`, [], {autoPage: true, prepare: true});
				const transitStream = new RowToTransit();
				utils.pipeWithErrors(rowStream, transitStream);
				utils.pipeWithErrors(transitStream, process.stdout);
				transitStream.once('end', resolve);
				transitStream.once('error', reject);
			});
		});
	});
}

class TransitToInsert extends Transform {
	constructor(client, stashName) {
		T(client, cassandra.Client, stashName, T.string);
		super({readableObjectMode: true});
		this._client         = client;
		this._stashName      = stashName;
		this._transitReader  = getTransitReader();
		this._columnsCreated = new Set();
	}

	async _insertFromLine(lineBuf) {
		const line = lineBuf.toString('utf-8');
		const obj = this._transitReader.read(line);

		function undefinedToNull(o, k) {
			if (o.get(k) === undefined) {
				o.set(k, null);
			}
		}
		undefinedToNull(obj, 'crc32c');
		undefinedToNull(obj, 'content');
		undefinedToNull(obj, 'version');
		undefinedToNull(obj, 'block_size');

		T(obj.get('basename'), T.string);
		T(obj.get('type'),     T.string);
		T(obj.get('mtime'),    Date);
		T(obj.get('parent'),   Buffer);
		A.eq(obj.get('parent').length, 128 / 8);

		if (obj.get('version') === null) {
			A.eq(obj.get('block_size'), null);
			if (obj.get('type') === 'f' && obj.get('content') === null) {
				obj.set('block_size', 0);
			}
			obj.set('version', 2);
		}

		if (obj.get('version') === 2) {
			if (obj.get('type') === 'f') {
				// Pre-version 3 rows don't have uuid for files, so we must add one.
				A.eq(obj.get('uuid'), null);
				obj.set('uuid', makeUuid());
			} else if (obj.get('type') === 'd') {
				T(obj.get('uuid'), Buffer);
			}

			// Pre-version 3 rows don't have added_ information, so add it now,
			// as if it were added by the current user on the current host.
			obj.set('added_time',    utils.dateNow());
			obj.set('added_user',    USERNAME);
			obj.set('added_host',    HOSTNAME);
			obj.set('added_version', TERASTASH_VERSION);
			obj.set('version',       3);
		}

		T(obj.get('added_time'),    Date);
		T(obj.get('added_user'),    T.string);
		T(obj.get('added_host'),    T.string);
		T(obj.get('added_version'), T.string);
		T(obj.get('uuid'),          Buffer);
		A.eq(obj.get('uuid').length, 128 / 8);

		const extraCols = [];
		const extraVals = [];
		let totalChunksSize = 0;
		for (const k of obj.keys()) {
			if (k.startsWith("chunks_in_") && obj.get(k, null) !== null) {
				if (!this._columnsCreated.has(k)) {
					await tryCreateColumnOnStashTable(
						this._client, this._stashName, k, 'list<frozen<chunk>>');
					this._columnsCreated.add(k);
				}
				const chunksArray = obj.get(k).map(function(v) {
					totalChunksSize += Number(v.get('size'));
					return transit.mapToObject(v);
				});
				extraCols.push(k);
				extraVals.push(chunksArray);
			}
		}

		if (obj.get('type') === 'f') {
			if (obj.get('crc32c') === null) {
				if (obj.get('content') !== null) {
					// Generate crc32c for version null dumps, which have blake2b224
					// instead of crc32c.
					T(obj.get('content'), Buffer);
					obj.set('crc32c', hasher.crcToBuf(sse4_crc32.calculate(obj.get('content'))));
				}
			} else {
				T(obj.get('crc32c'), Buffer);
			}

			T(obj.get('size'), cassandra.types.Long);
			utils.assertSafeNonNegativeLong(obj.get('size'));
			const size = Number(obj.get('size'));

			if (obj.get('content') === null) {
				T(obj.get('key'), Buffer);
				A.eq(obj.get('key').length, 128 / 8);

				// Check size
				const sizeOfTags =
					obj.get('block_size') > 0 ?
						GCM_TAG_SIZE * Math.ceil(size / obj.get('block_size')) :
						0;
				const sizeWithTags  = size + sizeOfTags;
				const concealedSize = utils.concealSize(sizeWithTags);
				A.eq(concealedSize, totalChunksSize);
			} else {
				A.eq(size, obj.get('content').length);
			}
			T(obj.get('executable'), T.boolean);
		} else if (obj.get('type') === 'd') {
			A.eq(obj.get('content'),    null);
			A.eq(obj.get('executable'), null);
			A.eq(obj.get('crc32c'),     null);
			A.eq(obj.get('size'),       null);
		}

		const cols = [
			'basename', 'parent', 'type', 'uuid', 'content', 'key', 'size',
			'crc32c', 'mtime', 'executable', 'version', 'block_size',
			'added_time', 'added_user', 'added_host', 'added_version'];
		const vals   = cols.map(key => obj.get(key));
		const qMarks = utils.filledArray(cols.length + extraCols.length, "?");
		const query  = `INSERT INTO "${KEYSPACE_PREFIX + this._stashName}".fs
			(${utils.colsAsString(cols.concat(extraCols))})
			VALUES (${qMarks.join(", ")});`;
		await runQuery(this._client, query, vals.concat(extraVals));
		return obj;
	}

	_transform(lineBuf, encoding, callback) {
		T(lineBuf, Buffer);
		try {
			const p = this._insertFromLine(lineBuf);
			p.then(function _insertFromLine$callback(obj) {
				callback(null, obj);
			}, function _insertFromLine$errback(err) {
				callback(err);
			});
		} catch(err) {
			callback(err);
		}
	}
}

function importDb(outCtx, stashName, dumpFile) {
	T(outCtx, OutputContextType, stashName, T.string, dumpFile, T.string);
	if (outCtx.mode !== 'quiet') {
		console.log(`Restoring from ${dumpFile === '-' ? 'stdin' : inspect(dumpFile)} into stash ${inspect(stashName)}.`);
		console.log('Note that files may be restored before directories, so you might ' +
			'not see anything in the stash until the restore process is complete.');
	}
	return doWithClient(getNewClient(), async function importDb$coro(client) {
		let inputStream;
		if (dumpFile === '-') {
			inputStream = process.stdin;
		} else {
			inputStream = fs.createReadStream(dumpFile);
		}
		const lineStream = new line_reader.DelimitedBufferDecoder(Buffer.from("\n"));
		utils.pipeWithErrors(inputStream, lineStream);
		// 4 requests in flight saturates a 4790K core (tested io.js 3.2.0/V8 4.4)
		const workStealers = work_stealer.makeWorkStealers(lineStream, 4);
		let count = 0;

		const start = Date.now();
		function printProgress() {
			utils.clearOrLF(process.stdout);
			process.stdout.write(`${commaify(count)}/? done at ` +
				`${commaify(Math.round(count/((Date.now() - start) / 1000)))}/sec...`);
		}

		const inserters = workStealers.map(function(stealer) {
			const inserter = new TransitToInsert(client, stashName);
			stealer.pipe(inserter);

			inserter.on('data', function(_obj) {
				count += 1;
				// Print every 100th to avoid getting 30% slowdown by just terminal output
				if (outCtx.mode === 'terminal' && count % 100 === 0) {
					printProgress();
				} else if (outCtx.mode === 'log' && count % 1000 === 0) {
					printProgress();
				}
			});

			return new Promise(function(resolve, reject) {
				inserter.once('end', resolve);
				inserter.once('error', reject);
			});
		});
		await Promise.all(inserters);
		if (outCtx.mode !== 'quiet') {
			printProgress();
			console.log('\nDone importing.');
		}
	});
}

module.exports = {
	TERASTASH_VERSION, getNewClient,
	initStash, destroyStash, getStashes, getChunkStores, authorizeGDrive, updateGoogleTokens,
	listTerastashKeyspaces, listChunkStores, defineChunkStore, configChunkStore,
	addFile, addFiles, streamFile, getFile, getFiles, catFile, catFiles, catRangedFiles,
	dropFile, dropFiles, shooFile, shooFiles, moveFiles, makeDirectories, lsPath,
	findPath, infoFile, infoFiles, KEYSPACE_PREFIX, exportDb, importDb,
	DirectoryNotEmptyError, NotInWorkingDirectoryError, NoSuchPathError,
	NotAFileError, PathAlreadyExistsError, KeyspaceMissingError,
	DifferentStashesError, UnexpectedFileError, UsageError, FileChangedError,
	checkChunkSize, chunksToBlockRanges, getChildrenForParent, getUuidForPath,
	getRowByPath, getRowByParentBasename, getStashInfoByName, createTeamDrive
};
