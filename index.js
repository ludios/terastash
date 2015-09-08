"use strong";
"use strict";

const A = require('ayy');
const T = require('notmytype');
const Promise = require('bluebird');
const fs = require('./fs-promisified');
const mkdirpAsync = Promise.promisify(require('mkdirp'));
const path = require('path');
const crypto = require('crypto');
const chalk = require('chalk');
const inspect = require('util').inspect;
const streamifier = require('streamifier');
const noop = require('lodash.noop');
const Transform = require('stream').Transform;

const utils = require('./utils');
const filename = require('./filename');
const commaify = utils.commaify;
const LazyModule = utils.LazyModule;
const loadNow = utils.loadNow;
const OutputContextType = utils.OutputContextType;
const compile_require = require('./compile_require');
const RetryPolicy = require('cassandra-driver/lib/policies/retry').RetryPolicy;
const deepEqual = require('deep-equal');
const Table = require('cli-table');

let CassandraClientType = T.object;

let aes = new LazyModule('./aes');
let hasher = new LazyModule('./hasher');
let cassandra;
cassandra = new LazyModule('cassandra-driver', require, function(realModule) {
	CassandraClientType = realModule.Client;
});
let localfs = new LazyModule('./chunker/localfs');
let gdrive = new LazyModule('./chunker/gdrive');
let sse4_crc32 = new LazyModule('sse4_crc32', compile_require);
let readline = new LazyModule('readline');
let padded_stream = new LazyModule('./padded_stream');
let line_reader = new LazyModule('./line_reader');
let work_stealer = new LazyModule('./work_stealer');
let transit = new LazyModule('transit-js');

const KEYSPACE_PREFIX = "ts_";


class CustomRetryPolicy extends RetryPolicy {
	onReadTimeout(requestInfo, consistency, received, blockFor, isDataPresent) {
		if(requestInfo.nbRetry > 10) {
			return this.rethrowResult();
		}
		return this.retryResult();
	}

	onWriteTimeout(requestInfo, consistency, received, blockFor, writeType) {
		if(requestInfo.nbRetry > 10) {
			return this.rethrowResult();
		}
		// We assume it's safe to retry our writes
		return this.retryResult();
	}
}

function getNewClient() {
	cassandra = loadNow(cassandra);
	return new cassandra.Client({
		contactPoints: ['localhost'],
		policies: {
			retry: new CustomRetryPolicy()
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
const getStashInfoByPath = Promise.coroutine(function* getStashInfoByPath$coro(pathname) {
	T(pathname, T.string);
	const config = yield getStashes();
	if(!config.stashes || typeof config.stashes !== "object") {
		throw new Error(`terastash config has no "stashes" or not an object`);
	}

	const resolvedPathname = path.resolve(pathname);
	for(const stashName of Object.keys(config.stashes)) {
		const stash = config.stashes[stashName];
		//console.log(resolvedPathname, stash.path);
		if(resolvedPathname === stash.path || resolvedPathname.startsWith(stash.path + path.sep)) {
			stash.name = stashName;
			return stash;
		}
	}
	throw new NotInWorkingDirectoryError(
		`File ${inspect(pathname)} is not in a terastash working directory`);
});

/**
 * Return a stash for a given stash name
 */
const getStashInfoByName = Promise.coroutine(function* getStashInfoByName$coro(stashName) {
	T(stashName, T.string);
	const config = yield getStashes();
	if(!config.stashes || typeof config.stashes !== "object") {
		throw new Error(`terastash config has no "stashes" or not an object`);
	}

	const stash = config.stashes[stashName];
	if(!stash) {
		throw new Error(`No stash with name ${stashName}`);
	}
	stash.name = stashName;
	return stash;
});

/**
 * For any given relative user path, which may include ../, return
 * the corresponding path that should be used in the Cassandra
 * database.
 */
function userPathToDatabasePath(base, p) {
	T(base, T.string, p, T.string);
	const resolved = path.resolve(p);
	if(resolved === base) {
		return "";
	} else {
		const dbPath = resolved.replace(base + "/", "").replace(/\\/g, "/");
		A(!dbPath.startsWith('/'), dbPath);
		return dbPath;
	}
}

/**
 * If stashName === null, convert p to database path, else just return p.
 */
function eitherPathToDatabasePath(stashName, base, p) {
	T(stashName, T.maybe(T.string), base, T.string, p, T.string);
	if(stashName === null) {
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
function runQuery(client, statement, args) {
	T(client, CassandraClientType, statement, T.string, args, T.optional(Array));
	//console.log(`runQuery(${client}, ${inspect(statement)}, ${inspect(args)})`);
	return new Promise(function runQuery$Promise(resolve, reject) {
		client.execute(statement, args, {prepare: true}, function(err, result) {
			if(err) {
				reject(err);
			} else {
				resolve(result);
			}
		});
	}).catch(function runQuery$catch(err) {
		if(isKeyspaceMissingError(err)) {
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
	T(client, CassandraClientType, f, T.function);
	const p = f(client);
	function shutdown(ret) {
		try {
			client.shutdown();
		} catch(e) {
			console.log("client.shutdown() failed:");
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

const doWithPath = Promise.coroutine(function* doWithPath$coro(stashName, p, fn) {
	T(stashName, T.maybe(T.string), p, T.string, fn, T.function);
	const resolvedPathname = path.resolve(p);
	let dbPath;
	let stashInfo;
	if(stashName) { // Explicit stash name provided
		stashInfo = yield getStashInfoByName(stashName);
		dbPath = p;
	} else {
		stashInfo = yield getStashInfoByPath(resolvedPathname);
		dbPath = userPathToDatabasePath(stashInfo.path, p);
	}

	const parentPath = utils.getParentPath(dbPath);
	A(!parentPath.startsWith('/'), parentPath);

	// TODO: validate stashInfo.name - it may contain injection
	return fn(stashInfo, dbPath, parentPath);
});

const pathsorterAsc = utils.comparedBy(function(row) {
	return row.basename;
});

const pathsorterDesc = utils.comparedBy(function(row) {
	return row.basename;
}, true);

const mtimeSorterAsc = utils.comparedBy(function(row) {
	return row.mtime;
});

const mtimeSorterDesc = utils.comparedBy(function(row) {
	return row.mtime;
}, true);

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

class DifferentStashesError extends Error {
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

const getRowByParentBasename = Promise.coroutine(function* getRowByParentBasename$coro(client, stashName, parent, basename, cols) {
	T(client, CassandraClientType, stashName, T.string, parent, Buffer, basename, T.string, cols, utils.ColsType);
	const result = yield runQuery(
		client,
		`SELECT ${utils.colsAsString(cols)}
		from "${KEYSPACE_PREFIX + stashName}".fs
		WHERE parent = ? AND basename = ?`,
		[parent, basename]
	);
	A.lte(result.rows.length, 1);
	if(!result.rows.length) {
		throw new NoSuchPathError(
			`No entry with parent=${parent.toString('hex')}` +
			` and basename=${inspect(basename)}`);
	}
	return result.rows[0];
});

let getUuidForPath;
getUuidForPath = Promise.coroutine(function* getUuidForPath$coro(client, stashName, p) {
	T(client, CassandraClientType, stashName, T.string, p, T.string);
	if(p === "") {
		// root directory is 0
		return new Buffer(128/8).fill(0);
	}

	const parentPath = utils.getParentPath(p);
	const parent = yield getUuidForPath(client, stashName, parentPath);
	const basename = p.split("/").pop();

	const row = yield getRowByParentBasename(client, stashName, parent, basename, ['type', 'uuid']);
	if(row.type !== "d") {
		throw new NoSuchPathError(`${inspect(p)} in ${stashName} is not a directory`);
	}
	T(row.uuid, Buffer);
	return row.uuid;
});

const getRowByPath = Promise.coroutine(function* getRowByPath$coro(client, stashName, p, cols) {
	T(client, CassandraClientType, stashName, T.string, p, T.string, cols, utils.ColsType);
	const parentPath = utils.getParentPath(p);
	const parent = yield getUuidForPath(client, stashName, parentPath);
	const basename = p.split("/").pop();
	return getRowByParentBasename(client, stashName, parent, basename, cols);
});

const getChildrenForParent = Promise.coroutine(function* getChildrenForParent$coro(client, stashName, parent, cols, limit) {
	T(client, CassandraClientType, stashName, T.string, parent, Buffer, cols, utils.ColsType, limit, T.optional(T.number));

	const rows = [];
	const rowStream = client.stream(
		`SELECT ${utils.colsAsString(cols)}
		from "${KEYSPACE_PREFIX + stashName}".fs
		WHERE parent = ?
		${limit === undefined ? "" : `LIMIT ${limit}`}`,
		[parent], {autoPage: true, prepare: true}
	);
	rowStream.on('readable', function getChildForParent$rowStream$readable() {
		let row;
		while(row = this.read()) {
			rows.push(row);
		}
	});
	return new Promise(function getChildForParent$Promise(resolve, reject) {
		rowStream.on('end', function getChildForParent$rowStream$end() {
			resolve(rows);
		});
		rowStream.once('error', reject);
	});
});

function lsPath(stashName, options, p) {
	return doWithClient(getNewClient(), function lsPath$doWithClient(client) {
		return doWithPath(stashName, p, Promise.coroutine(function* lsPath$coro(stashInfo, dbPath, parentPath) {
			const parent = yield getUuidForPath(client, stashInfo.name, dbPath);
			const rows = yield getChildrenForParent(
				client, stashInfo.name, parent,
				["basename", "type", "size", "mtime", "executable"]
			);
			if(options.sortByMtime) {
				rows.sort(options.reverse ? mtimeSorterAsc : mtimeSorterDesc);
			} else {
				rows.sort(options.reverse ? pathsorterDesc : pathsorterAsc);
			}
			for(const row of rows) {
				A(!/[\r\n]/.test(row.basename), `${inspect(row.basename)} contains CR or LF`);
				if(options.justNames) {
					console.log(row.basename);
				} else {
					let decoratedName = row.basename;
					if(row.type === 'd') {
						decoratedName = chalk.bold.blue(decoratedName);
						decoratedName += '/';
					} else if(row.executable) {
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
		}));
	});
}

let listRecursively;
listRecursively = Promise.coroutine(function* listRecursively$coro(client, stashInfo, baseDbPath, dbPath, print0, type) {
	T(client, CassandraClientType, stashInfo, T.object, dbPath, T.string, print0, T.boolean, type, T.optional(T.string));
	const parent = yield getUuidForPath(client, stashInfo.name, dbPath);
	const rows = yield getChildrenForParent(
		client, stashInfo.name, parent,
		["basename", "type"]
	);
	rows.sort(pathsorterAsc);
	for(const row of rows) {
		A(!/[\r\n]/.test(row.basename), `${inspect(row.basename)} contains CR or LF`);
		let fullPath = `${dbPath}/${row.basename}`;
		if(type === undefined || type === row.type) {
			const pathWithoutBase = fullPath.replace(baseDbPath + "/", "");
			process.stdout.write(pathWithoutBase + (print0 ? "\0" : "\n"));
		}
		if(row.type === "d") {
			yield listRecursively(client, stashInfo, baseDbPath, fullPath, print0, type);
		}
	}
});

// Like "find" utility
function findPath(stashName, p, options) {
	T(stashName, T.maybe(T.string), p, T.string, options, T.object);
	return doWithClient(getNewClient(), function lsPath$doWithClient(client) {
		return doWithPath(stashName, p, Promise.coroutine(function* findPath$coro(stashInfo, dbPath, parentPath) {
			yield listRecursively(client, stashInfo, dbPath, dbPath, options.print0, options.type);
		}));
	});
}

const MISSING = Symbol('MISSING');
const DIRECTORY = Symbol('DIRECTORY');
const FILE = Symbol('FILE');

const getTypeInDbByParentBasename = Promise.coroutine(function* getTypeInDbByParentBasename$coro(client, stashName, parent, basename) {
	T(client, CassandraClientType, stashName, T.string, parent, Buffer, basename, T.string);
	let row;
	try {
		row = yield getRowByParentBasename(client, stashName, parent, basename, ['type']);
	} catch(err) {
		if(!(err instanceof NoSuchPathError)) {
			throw err;
		}
		return MISSING;
	}
	if(row.type === "f") {
		return FILE;
	} else if(row.type === "d") {
		return DIRECTORY;
	} else {
		throw new Error(
			`Unexpected type in db for parent=${parent.toString('hex')}` +
			` basename=${inspect(basename)}: ${inspect(row.type)}`
		);
	}
});

const getTypeInDbByPath = Promise.coroutine(function* getTypeInDbByPath$coro(client, stashName, dbPath) {
	T(client, CassandraClientType, stashName, T.string, dbPath, T.string);
	if(dbPath === "") {
		// The root directory
		return DIRECTORY;
	}
	const parent = yield getUuidForPath(client, stashName, utils.getParentPath(dbPath));
	return getTypeInDbByParentBasename(client, stashName, parent, utils.getBaseName(dbPath));
});

const getTypeInWorkingDirectory = Promise.coroutine(function* getTypeInWorkingDirectory$coro(p) {
	T(p, T.string);
	try {
		const stat = yield fs.statAsync(p);
		if(stat.isDirectory()) {
			return DIRECTORY;
		} else {
			return FILE;
		}
	} catch(err) {
		if(err.code !== 'ENOENT') {
			throw err;
		}
		return MISSING;
	}
});

class PathAlreadyExistsError extends Error {
	get name() {
		return this.constructor.name;
	}
}

function checkDbPath(dbPath) {
	T(dbPath, T.string);
	if(!dbPath) {
		// Empty dbPath is OK; it means root directory
		return;
	}
	dbPath.split('/').map(filename.check);
}

let makeDirsInDb;
makeDirsInDb = Promise.coroutine(function* makeDirsInDb$coro(client, stashName, p, dbPath) {
	T(client, CassandraClientType, stashName, T.string, p, T.string, dbPath, T.string);
	checkDbPath(dbPath);
	let mtime = new Date();
	try {
		mtime = (yield fs.statAsync(p)).mtime;
	} catch(err) {
		if(err.code !== 'ENOENT') {
			throw err;
		}
	}
	const parentPath = utils.getParentPath(dbPath);
	if(parentPath) {
		yield makeDirsInDb(client, stashName, p, parentPath);
	}
	const typeInDb = yield getTypeInDbByPath(client, stashName, dbPath);
	if(typeInDb === MISSING) {
		const parentUuid = yield getUuidForPath(client, stashName, utils.getParentPath(dbPath));
		const uuid = makeUuid();
		yield runQuery(
			client,
			`INSERT INTO "${KEYSPACE_PREFIX + stashName}".fs
			(basename, parent, uuid, type, mtime) VALUES (?, ?, ?, ?, ?);`,
			[utils.getBaseName(dbPath), parentUuid, uuid, 'd', mtime]
		);
	} else if(typeInDb === FILE) {
		throw new PathAlreadyExistsError(
			`Cannot mkdir in database:` +
			` ${inspect(dbPath)} in stash ${inspect(stashName)}` +
			` already exists as a file`);
	} else if(typeInDb === DIRECTORY) {
		// do nothing
	}
});

const tryCreateColumnOnStashTable = Promise.coroutine(function* tryCreateColumnOnStashTable$coro(client, stashName, columnName, type) {
	T(client, CassandraClientType, stashName, T.string, columnName, T.string, type, T.string);
	try {
		yield runQuery(client,
			`ALTER TABLE "${KEYSPACE_PREFIX + stashName}".fs ADD
			"${columnName}" ${type}`
		);
	} catch(err) {
		if(!(/^ResponseError: Invalid column name.*conflicts with an existing column$/.test(String(err)))) {
			throw err;
		}
	}
});

function makeKey() {
	if(Number(process.env.TERASTASH_INSECURE_AND_DETERMINISTIC)) {
		const keyCounter = new utils.PersistentCounter(
			path.join(process.env.TERASTASH_COUNTERS_DIR, 'key-counter'));
		const buf = new Buffer(128/8).fill(0);
		buf.writeIntBE(keyCounter.getNext(), 0, 128/8);
		return buf;
	} else {
		return crypto.randomBytes(128/8);
	}
}

function makeUuid() {
	let uuid;
	if(Number(process.env.TERASTASH_INSECURE_AND_DETERMINISTIC)) {
		const uuidCounter = new utils.PersistentCounter(
			path.join(process.env.TERASTASH_COUNTERS_DIR, 'uuid-counter'), 1);
		const buf = new Buffer(128/8).fill(0);
		buf.writeIntBE(uuidCounter.getNext(), 0, 128/8);
		uuid = buf;
	} else {
		uuid = crypto.randomBytes(128/8);
	}
	A(
		!uuid.equals(new Buffer(128/8).fill(0)),
		"uuid must not be 0 because root directory is 0"
	);
	return uuid;
}

const getChunkStore = Promise.coroutine(function* getChunkStore$coro(stashInfo) {
	const storeName = stashInfo.chunkStore;
	if(!storeName) {
		throw new Error("stash info doesn't specify chunkStore key");
	}
	const config = yield getChunkStores();
	const chunkStore = config.stores[storeName];
	if(!chunkStore) {
		throw new Error(`Chunk store ${storeName} is not defined in chunk-stores.json`);
	}
	chunkStore.name = storeName;
	return chunkStore;
});

let selfTests;
selfTests = {aes: function selfTests$aes() {
	aes = loadNow(aes);
	aes.selfTest();
	selfTests.aes = noop;
}};

const getStashInfoForPaths = Promise.coroutine(function* getStashInfoForPaths$coro(paths) {
	// Make sure all paths are in the same stash
	const stashInfos = [];
	// Don't use Promise.all to avoid having too many file handles open
	for(const p of paths) {
		stashInfos.push(yield getStashInfoByPath(path.resolve(p)));
	}
	const stashNames = stashInfos.map(utils.prop('name'));
	if(!utils.allIdentical(stashNames)) {
		throw new DifferentStashesError(
			`All paths used in command must be in the same stash;` +
			` stashes were ${inspect(stashNames)}`);
	}
	return stashInfos[0];
});

const getStashInfoForNameOrPaths = Promise.coroutine(function* getStashInfoForNameOrPaths$coro(stashName, paths) {
	T(stashName, T.maybe(T.string), paths, T.list(T.string));
	if(stashName !== null) {
		return yield getStashInfoByName(stashName);
	} else {
		return yield getStashInfoForPaths(paths);
	}
});

const dropFile = Promise.coroutine(function* dropFile$coro(client, stashInfo, dbPath) {
	T(client, CassandraClientType, stashInfo, T.object, dbPath, T.string);
	const chunkStore = yield getChunkStore(stashInfo);
	const parentUuid = yield getUuidForPath(client, stashInfo.name, utils.getParentPath(dbPath));
	let chunks = null;
	try {
		const row = yield getRowByParentBasename(
			client, stashInfo.name, parentUuid, utils.getBaseName(dbPath),
			[`chunks_in_${chunkStore.name}`]
		);
		chunks = row[`chunks_in_${chunkStore.name}`];
	} catch(err) {
		if(!isColumnMissingError(err)) {
			throw err;
		}
	}
	const row = yield getRowByParentBasename(
		client, stashInfo.name, parentUuid, utils.getBaseName(dbPath),
		["type", "uuid"]
	);
	if(row.type === 'd') {
		const childRows = yield getChildrenForParent(client, stashInfo.name, row.uuid, ["basename"], 1);
		if(childRows.length) {
			throw new DirectoryNotEmptyError(
				`Refusing to drop ${inspect(dbPath)} because it is a non-empty directory`
			);
		}
	}
	// TODO: Instead of DELETE, mark file with 'deleting' or something in case
	// the chunk-deletion process needs to be resumed later.
	yield runQuery(
		client,
		`DELETE FROM "${KEYSPACE_PREFIX + stashInfo.name}".fs
		WHERE parent = ? AND basename = ?;`,
		[parentUuid, utils.getBaseName(dbPath)]
	);
	if(chunks !== null) {
		validateChunks(chunks);
		if(chunkStore.type === "localfs") {
			localfs = loadNow(localfs);
			yield localfs.deleteChunks(chunkStore.directory, chunks);
		} else {
			gdrive = loadNow(gdrive);
			const gdriver = new gdrive.GDriver(chunkStore.clientId, chunkStore.clientSecret);
			yield gdriver.loadCredentials();
			yield gdrive.deleteChunks(gdriver, chunks);
		}
	}
});

/**
 * Remove files from the Cassandra database and their corresponding chunks.
 */
function dropFiles(stashName, paths) {
	T(stashName, T.maybe(T.string), paths, T.list(T.string));
	return doWithClient(getNewClient(), Promise.coroutine(function* dropFiles$coro(client) {
		const stashInfo = yield getStashInfoForNameOrPaths(stashName, paths);
		for(const p of paths) {
			const dbPath = eitherPathToDatabasePath(stashName, stashInfo.path, p);
			yield dropFile(client, stashInfo, dbPath);
		}
	}));
}

// Does *not* include the length of the CRC32C itself
const CRC_BLOCK_SIZE = (8 * 1024) - 4;

function checkChunkSize(size) {
	T(size, T.number);
	// (CRC block size + CRC length) must be a multiple of chunkSize, for
	// implementation convenience.
	if(size % (CRC_BLOCK_SIZE + 4) !== 0) {
		throw new Error(`Chunk size must be a multiple of ` +
			`${CRC_BLOCK_SIZE + 4}; got ${size}`);
	}
	aes = loadNow(aes);
	// Chunk size must be a multiple of an AES block, for implementation convenience.
	A.eq(size % aes.BLOCK_SIZE, 0);
}

/**
 * Put file `p` into the Cassandra database as path `dbPath`.
 *
 * If `dropOldIfDifferent`, if the path in db already exists and the corresponding local
 * file has a different (mtime, size, executable), drop the db path and add the new file.
 */
const addFile = Promise.coroutine(function* addFile$coro(outCtx, client, stashInfo, p, dbPath, dropOldIfDifferent) {
	T(
		client, CassandraClientType,
		stashInfo, StashInfoType,
		p, T.string,
		dbPath, T.string,
		dropOldIfDifferent, T.optional(T.boolean)
	);
	checkDbPath(dbPath);

	let oldRow;
	const throwIfAlreadyInDb = Promise.coroutine(function* throwIfAlreadyInDb$coro() {
		let caught = false;
		try {
			oldRow = yield getRowByPath(client, stashInfo.name, dbPath, ['mtime', 'size', 'type', 'executable']);
		} catch(e) {
			if(!(e instanceof NoSuchPathError)) {
				throw e;
			}
			caught = true;
		}
		if(!caught) {
			throw new PathAlreadyExistsError(
				`Cannot add to database:` +
				` ${inspect(dbPath)} in stash ${inspect(stashInfo.name)}` +
				` already exists as a ${oldRow.type === 'd' ? "directory" : "file"}`);
		}
	});

	const stat = yield fs.statAsync(p);
	if(!stat.isFile()) {
		throw new Error(`Cannot add ${inspect(p)} because it is not a file`);
	}
	const type = 'f';
	const mtime = stat.mtime;
	const executable = Boolean(stat.mode & 0o100); /* S_IXUSR */
	const sticky = Boolean(stat.mode & 0o1000);
	if(sticky) {
		throw new UnexpectedFileError(
			`Refusing to add file ${inspect(p)} because it has sticky bit set,` +
			` which may have been set by 'ts shoo'`
		);
	}

	try {
		// Check early to avoid uploading to chunk store and doing other work
		yield throwIfAlreadyInDb();
	} catch(e) {
		if(!(e instanceof PathAlreadyExistsError) || !dropOldIfDifferent) {
			throw e;
		}
		// User wants to replace old file in db, but only if new file is different
		const newFile = {type: 'f', mtime, executable, size: stat.size};
		oldRow.size = Number(oldRow.size);
		//console.log({newFile, oldRow});
		if(!deepEqual(newFile, oldRow)) {
			const table = new Table({
				chars: {'mid': '', 'left-mid': '', 'mid-mid': '', 'right-mid': ''},
				head: ['which', 'mtime', 'size', 'executable']
			});
			table.push(['old', String(oldRow.mtime), commaify(oldRow.size), oldRow.executable]);
			table.push(['new', String(mtime), commaify(stat.size), executable]);
			console.log(`Notice: replacing ${inspect(dbPath)} in db\n${table.toString()}`);
			yield dropFile(client, stashInfo, dbPath);
		} else {
			throw e;
		}
	}

	const chunkStore = yield getChunkStore(stashInfo);
	let content = null;
	let chunkInfo;
	let size;
	// For file stored in chunk store, whole-file crc32c is not available.
	let crc32c = null;
	let key = null;

	if(stat.size >= stashInfo.chunkThreshold) {
		key = makeKey();

		aes = loadNow(aes);
		hasher = loadNow(hasher);
		padded_stream = loadNow(padded_stream);

		checkChunkSize(chunkStore.chunkSize);

		const sizeOfHashes = 4 * Math.ceil(stat.size / CRC_BLOCK_SIZE);
		const sizeWithHashes = stat.size + sizeOfHashes;
		utils.assertSafeNonNegativeInteger(sizeWithHashes);

		const concealedSize = utils.concealSize(sizeWithHashes);
		utils.assertSafeNonNegativeInteger(concealedSize);
		A.gte(concealedSize, sizeWithHashes);

		// Note: a 1GB chunk stores less than 1GB of data because of the CRC32C's
		// every 8188 bytes.
		const dataBytesPerChunk = chunkStore.chunkSize * (CRC_BLOCK_SIZE / (CRC_BLOCK_SIZE + 4));
		utils.assertSafeNonNegativeInteger(dataBytesPerChunk);
		let startData = -dataBytesPerChunk;
		let startChunk = -chunkStore.chunkSize;

		// getChunkStream is like a next() on an iterator, except caller can pass
		// in `true` to get the last chunk again.  This "rewinding" is necessary
		// because upload of a chunk may fail and need to be retried.   We don't
		// want to re-read the entire file just to continue with the chunk we need
		// again.
		const getChunkStream = Promise.coroutine(function* getChunkStream$coro(lastChunkAgain) {
			T(lastChunkAgain, T.boolean);

			if(!lastChunkAgain) {
				startData += dataBytesPerChunk;
				startChunk += chunkStore.chunkSize;
			}
			utils.assertSafeNonNegativeInteger(startData);
			utils.assertSafeNonNegativeInteger(startChunk);

			if(startChunk >= concealedSize) {
				// No more chunk streams
				return null;
			}

			// Ensure that file is still the same size before opening it again
			const statAgain = yield fs.statAsync(p);
			if(statAgain.size !== stat.size) {
				throw new FileChangedError(
					`Size of ${inspect(p)} changed from\n` +
					`${commaify(stat.size)} to\n${commaify(statAgain.size)}`
				);
			}
			if(statAgain.mtime.getTime() !== stat.mtime.getTime()) {
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

			const paddedStream = new padded_stream.Padder(
				Math.min(dataBytesPerChunk, concealedSize - sizeOfHashes - startData));
			utils.pipeWithErrors(inputStream, paddedStream);

			const hashedStream = new hasher.CRCWriter(CRC_BLOCK_SIZE);
			utils.pipeWithErrors(paddedStream, hashedStream);

			selfTests.aes();
			A.eq(startChunk % aes.BLOCK_SIZE, 0);
			const cipherStream = crypto.createCipheriv(
				'aes-128-ctr', key, aes.blockNumberToIv(startChunk / aes.BLOCK_SIZE));
			utils.pipeWithErrors(hashedStream, cipherStream);

			return cipherStream;
		});

		let _;
		if(chunkStore.type === "localfs") {
			localfs = loadNow(localfs);
			_ = yield localfs.writeChunks(outCtx, chunkStore.directory, getChunkStream);
		} else if(chunkStore.type === "gdrive") {
			gdrive = loadNow(gdrive);
			const gdriver = new gdrive.GDriver(chunkStore.clientId, chunkStore.clientSecret);
			yield gdriver.loadCredentials();
			_ = yield gdrive.writeChunks(outCtx, gdriver, chunkStore.parents, getChunkStream);
		} else {
			throw new Error(`Unknown chunk store type ${inspect(chunkStore.type)}`);
		}

		const totalSize = _[0];
		chunkInfo = _[1];
		for(const info of chunkInfo) {
			A.lte(info.size, chunkStore.chunkSize, `uploaded a too-big chunk:\n${inspect(info)}`);
			info.version = 2;
			info.block_size = CRC_BLOCK_SIZE;
		}
		A.eq(totalSize, concealedSize,
			`For ${dbPath}, wrote to chunks\n` +
			`${commaify(totalSize)} bytes instead of the expected\n` +
			`${commaify(concealedSize)} (concealed) bytes`);
		T(chunkInfo, Array);

		size = stat.size;
	} else {
		content = yield fs.readFileAsync(p);
		hasher = loadNow(hasher);
		sse4_crc32 = loadNow(sse4_crc32);
		crc32c = hasher.crcToBuf(sse4_crc32.calculate(content));
		size = content.length;
		A.eq(size, stat.size,
			`For ${dbPath}, read\n` +
			`${commaify(size)} bytes instead of the expected\n` +
			`${commaify(stat.size)} bytes; did file change during reading?`);
	}

	const insert = Promise.coroutine(function* insert$coro() {
		const parentPath = utils.getParentPath(dbPath);
		if(parentPath) {
			yield makeDirsInDb(client, stashInfo.name, path.dirname(p), parentPath);
		}
		// TODO: make makeDirsInDb return uuid so that we don't have to get it again
		const parentUuid = yield getUuidForPath(client, stashInfo.name, parentPath);
		return yield runQuery(
			client,
			`INSERT INTO "${KEYSPACE_PREFIX + stashInfo.name}".fs
			(basename, parent, type, content, key, "chunks_in_${chunkStore.name}", size, crc32c, mtime, executable)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
			[utils.getBaseName(dbPath), parentUuid, type, content, key, chunkInfo, size, crc32c, mtime, executable]
		);
	});

	// Check again to narrow the race condition
	yield throwIfAlreadyInDb();
	try {
		yield insert();
	} catch(err) {
		if(!isColumnMissingError(err)) {
			throw err;
		}
		yield tryCreateColumnOnStashTable(
			client, stashInfo.name, `chunks_in_${chunkStore.name}`, 'list<frozen<chunk>>');
		yield throwIfAlreadyInDb();
		yield insert();
	}
});

/**
 * Put files or directories into the Cassandra database.
 */
function addFiles(outCtx, paths, continueOnExists, dropOldIfDifferent) {
	T(
		outCtx, OutputContextType,
		paths, T.list(T.string),
		continueOnExists, T.optional(T.boolean),
		dropOldIfDifferent, T.optional(T.boolean)
	);
	return doWithClient(getNewClient(), Promise.coroutine(function* addFiles$coro(client) {
		const stashInfo = yield getStashInfoForPaths(paths);

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
			for(const p of paths) {
				if(outCtx.mode === 'terminal') {
					utils.clearOrLF(process.stdout);
					process.stdout.write(`${count}/${paths.length}...`);
				}
				const dbPath = userPathToDatabasePath(stashInfo.path, p);
				try {
					yield addFile(outCtx, client, stashInfo, p, dbPath, dropOldIfDifferent);
				} catch(err) {
					if(!(err instanceof PathAlreadyExistsError ||
						err instanceof UnexpectedFileError /* was sticky */)
					|| !continueOnExists) {
						throw err;
					}
					console.error(chalk.red(err.message));
				}
				if(stopNow) {
					break;
				}
				count++;
			}
		} finally {
			process.removeListener('SIGINT', stopSoon);
		}
	}));
}

function validateChunks(chunks) {
	T(chunks, utils.ChunksType);
	let expectIdx = 0;
	for(const chunk of chunks) {
		A.eq(chunk.idx, expectIdx, "Bad chunk data from database");
		expectIdx += 1;
	}
}

const makeEmptySparseFile = Promise.coroutine(function* makeEmptySparseFile$coro(p, size) {
	T(p, T.string, size, T.number);
	// First delete the existing file because it may have hard links, and we
	// don't want to overwrite the content of said hard links.
	yield utils.tryUnlink(p);
	const handle = yield fs.openAsync(p, "w");
	try {
		yield fs.truncateAsync(handle, size);
	} finally {
		yield fs.closeAsync(handle);
	}
});

const makeFakeFile = Promise.coroutine(function* makeEmptySparseFile$coro(p, size, mtime) {
	T(p, T.string, size, T.number, mtime, Date);
	yield makeEmptySparseFile(p, size);
	yield utils.utimesMilliseconds(p, mtime, mtime);
	// TODO: do this without a stat?
	const stat = yield fs.statAsync(p);
	const withSticky = stat.mode | 0o1000;
	yield fs.chmodAsync(p, withSticky);
});

const shooFile = Promise.coroutine(function* shooFile$coro(client, stashInfo, p) {
	T(client, CassandraClientType, stashInfo, StashInfoType, p, T.string);
	const dbPath = userPathToDatabasePath(stashInfo.path, p);
	const row = yield getRowByPath(client, stashInfo.name, dbPath, ['mtime', 'size', 'type']);
	if(row.type === 'd') {
		throw new NotAFileError(`Can't shoo dbPath=${inspect(dbPath)}; it is a directory`);
	} else if(row.type === 'f') {
		const stat = yield fs.statAsync(p);
		T(stat.mtime, Date);
		if(stat.mtime.getTime() !== Number(row.mtime)) {
			throw new UnexpectedFileError(
				`mtime for working directory file ${inspect(p)} is \n${stat.mtime.toISOString()}` +
				` but mtime for dbPath=${inspect(dbPath)} is` +
				`\n${new Date(Number(row.mtime)).toISOString()}`
			);
		}
		T(stat.size, T.number);
		if(stat.size !== Number(row.size)) {
			throw new UnexpectedFileError(
				`size for working directory file ${inspect(p)} is \n${commaify(stat.size)}` +
				` but size for dbPath=${inspect(dbPath)} is \n${commaify(Number(row.size))}`
			);
		}
		yield makeFakeFile(p, stat.size, row.mtime);
	} else {
		throw new Error(`Unexpected type ${inspect(row.type)} for dbPath=${inspect(dbPath)}`);
	}
});

function shooFiles(paths, continueOnError) {
	T(paths, T.list(T.string), continueOnError, T.optional(T.boolean));
	return doWithClient(getNewClient(), Promise.coroutine(function* shooFiles$coro(client) {
		const stashInfo = yield getStashInfoForPaths(paths);
		for(const p of paths) {
			try {
				yield shooFile(client, stashInfo, p);
			} catch(err) {
				if(!(err instanceof UnexpectedFileError ||
					err instanceof NoSuchPathError)
				|| !continueOnError) {
					throw err;
				}
				console.error(chalk.red(err.message));
			}
		}
	}));
}

/**
 * Get a readable stream with the file contents, whether the file is in the db
 * or in a chunk store.
 */
const streamFile = Promise.coroutine(function* streamFile$coro(client, stashInfo, dbPath) {
	T(client, CassandraClientType, stashInfo, T.object, dbPath, T.string);
	// TODO: instead of checking just this one stash, check all stashes
	const storeName = stashInfo.chunkStore;
	if(!storeName) {
		throw new Error("stash info doesn't specify chunkStore key");
	}

	let row;
	try {
		row = yield getRowByPath(client, stashInfo.name, dbPath,
			["size", "type", "key", `chunks_in_${storeName}`, "crc32c", "content", "mtime", "executable"]
		);
	} catch(err) {
		if(!isColumnMissingError(err)) {
			throw err;
		}
		// chunks_in_${storeName} doesn't exist, try the query without it
		row = yield getRowByPath(client, stashInfo.name, dbPath,
			["size", "type", "key", "crc32c", "content", "mtime", "executable"]
		);
	}
	if(row.type !== 'f') {
		throw new NotAFileError(`Path ${inspect(dbPath)} in stash ${inspect(stashInfo.name)} is not a file`);
	}

	const chunkStore = (yield getChunkStores()).stores[storeName];
	const chunks = row[`chunks_in_${storeName}`];
	let bytesRead = 0;
	let unpaddedStream;
	if(chunks !== null) {
		validateChunks(chunks);
		A.eq(row.content, null);
		A.eq(row.key.length, 128/8);
		let cipherStream;
		if(chunkStore.type === "localfs") {
			localfs = loadNow(localfs);
			const chunksDir = chunkStore.directory;
			cipherStream = localfs.readChunks(chunksDir, chunks);
		} else if(chunkStore.type === "gdrive") {
			gdrive = loadNow(gdrive);
			const gdriver = new gdrive.GDriver(chunkStore.clientId, chunkStore.clientSecret);
			yield gdriver.loadCredentials();
			cipherStream = gdrive.readChunks(gdriver, chunks);
		} else {
			throw new Error(`Unknown chunk store type ${inspect(chunkStore.type)}`);
		}
		selfTests.aes();
		const clearStream = crypto.createCipheriv('aes-128-ctr', row.key, aes.blockNumberToIv(0));
		utils.pipeWithErrors(cipherStream, clearStream);

		hasher = loadNow(hasher);
		const unhashedStream = new hasher.CRCReader(CRC_BLOCK_SIZE);
		utils.pipeWithErrors(clearStream, unhashedStream);

		padded_stream = loadNow(padded_stream);
		unpaddedStream = new padded_stream.Unpadder(Number(row.size));
		utils.pipeWithErrors(unhashedStream, unpaddedStream);

		unpaddedStream.on('data', function(data) {
			bytesRead += data.length;
		});
		// We attached a 'data' handler, but don't let that put us into
		// flowing mode yet, because the user hasn't attached their own
		// 'data' handler yet.
		unpaddedStream.pause();
	} else {
		hasher = loadNow(hasher);
		sse4_crc32 = loadNow(sse4_crc32);
		unpaddedStream = streamifier.createReadStream(row.content);
		bytesRead = row.content.length;
		const crc32c = hasher.crcToBuf(sse4_crc32.calculate(row.content));
		// Note: only in-db content has a crc32c for entire file content
		if(!crc32c.equals(row.crc32c)) {
			unpaddedStream.emit('error', new Error(
				`For dbPath=${dbPath}, CRC32C is allegedly\n` +
				`${row.crc32c.toString('hex')} but CRC32C of data is\n` +
				`${crc32c.toString('hex')}`
			));
		}
	}
	unpaddedStream.once('end', function streamFile$end() {
		if(bytesRead !== Number(row.size)) {
			unpaddedStream.emit('error', new Error(
				`For dbPath=${dbPath}, expected length of content to be\n` +
				`${commaify(row.size)} but was\n` +
				`${commaify(bytesRead)}`
			));
		}
	});
	return [row, unpaddedStream];
});

/**
 * Get a file or directory from the Cassandra database.
 */
const getFile = Promise.coroutine(function* getFile$coro(client, stashInfo, dbPath, outputFilename, fake) {
	T(client, CassandraClientType, stashInfo, T.object, dbPath, T.string, outputFilename, T.string, fake, T.boolean);

	const _ = yield streamFile(client, stashInfo, dbPath);
	const row = _[0];
	const readStream = _[1];

	yield mkdirpAsync(path.dirname(outputFilename));

	// Delete the existing file because it may
	// 1) have hard links
	// 2) have the sticky bit set
	// 3) have other unwanted permissions set
	yield utils.tryUnlink(outputFilename);

	if(fake) {
		yield makeFakeFile(outputFilename, Number(row.size), row.mtime);
	} else {
		const writeStream = fs.createWriteStream(outputFilename);
		utils.pipeWithErrors(readStream, writeStream);
		yield new Promise(function getFiles$Promise(resolve, reject) {
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
		yield utils.utimesMilliseconds(outputFilename, row.mtime, row.mtime);
		if(row.executable) {
			// TODO: setting for 0o700 instead?
			yield fs.chmodAsync(outputFilename, 0o770);
		}
	}
});

function getFiles(stashName, paths, fake) {
	T(stashName, T.maybe(T.string), paths, T.list(T.string), fake, T.boolean);
	return doWithClient(getNewClient(), Promise.coroutine(function* getFiles$coro(client) {
		const stashInfo = yield getStashInfoForNameOrPaths(stashName, paths);
		for(const p of paths) {
			let dbPath;
			let outputFilename;
			// If stashName was given, write file to current directory
			if(stashName) {
				dbPath = p;
				outputFilename = p;
			} else {
				dbPath = userPathToDatabasePath(stashInfo.path, p);
				outputFilename = stashInfo.path + '/' + dbPath;
			}

			yield getFile(client, stashInfo, dbPath, outputFilename, fake);
		}
	}));
}

const catFile = Promise.coroutine(function* catFile$coro(client, stashInfo, dbPath) {
	T(client, CassandraClientType, stashInfo, T.object, dbPath, T.string);
	const _ = yield streamFile(client, stashInfo, dbPath);
	//const row = _[0];
	const readStream = _[1];
	utils.pipeWithErrors(readStream, process.stdout);
	yield new Promise(function(resolve, reject) {
		readStream.on('end', resolve);
		readStream.once('error', reject);
	});
});

function catFiles(stashName, paths) {
	T(stashName, T.maybe(T.string), paths, T.list(T.string));
	return doWithClient(getNewClient(), Promise.coroutine(function* catFiles$coro(client) {
		const stashInfo = yield getStashInfoForNameOrPaths(stashName, paths);
		for(const p of paths) {
			const dbPath = eitherPathToDatabasePath(stashName, stashInfo.path, p);
			yield catFile(client, stashInfo, dbPath);
		}
	}));
}

function makeDirectories(stashName, paths) {
	T(stashName, T.maybe(T.string), paths, T.list(T.string));
	return doWithClient(getNewClient(), Promise.coroutine(function* makeDirectories$coro(client) {
		let dbPaths;
		let stashInfo;
		if(stashName) { // Explicit stash name provided
			stashInfo = yield getStashInfoByName(stashName);
			dbPaths = paths;
		} else {
			stashInfo = yield getStashInfoForPaths(paths);
			dbPaths = paths.map(function(p) {
				return userPathToDatabasePath(stashInfo.path, p);
			});
		}
		for(let i=0; i < dbPaths.length; i++) {
			const p = paths[i];
			const dbPath = dbPaths[i];
			checkDbPath(dbPath);
			try {
				yield mkdirpAsync(p);
			} catch(err) {
				if(err.code !== 'EEXIST') {
					throw err;
				}
				throw new PathAlreadyExistsError(
					`Cannot mkdir in working directory:` +
					` ${inspect(p)} already exists and is not a directory`
				);
			}
			yield makeDirsInDb(client, stashInfo.name, p, dbPath);
		}
	}));
}

function moveFiles(stashName, sources, dest) {
	T(stashName, T.maybe(T.string), sources, T.list(T.string), dest, T.string);
	return doWithClient(getNewClient(), Promise.coroutine(function* moveFiles$coro(client) {
		let stashInfo;
		let dbPathSources;
		let dbPathDest;
		if(stashName) { // Explicit stash name provided
			stashInfo = yield getStashInfoByName(stashName);
			dbPathSources = sources;
			dbPathDest = dest;
		} else {
			stashInfo = yield getStashInfoForPaths(sources.concat(dest));
			dbPathSources = sources.map(function(p) {
				return userPathToDatabasePath(stashInfo.path, p);
			});
			dbPathDest = userPathToDatabasePath(stashInfo.path, dest);
		}
		checkDbPath(dbPathDest);

		// This is inherently racy; type may be different by the time we mv
		let destTypeInDb = yield getTypeInDbByPath(client, stashInfo.name, dbPathDest);
		// TODO XXX: is this right? what about when -n is specified?
		const destInWorkDir = path.join(stashInfo.path, dbPathDest);
		const destTypeInWorkDir = yield getTypeInWorkingDirectory(destInWorkDir);

		if(destTypeInDb === MISSING && destTypeInWorkDir === DIRECTORY) {
			yield makeDirsInDb(client, stashInfo.name, dest, dbPathDest);
			destTypeInDb = DIRECTORY;
		}

		if(destTypeInDb === FILE) {
			throw new PathAlreadyExistsError(
				`Cannot mv in database: destination ${inspect(dbPathDest)}` +
				` already exists in stash ${inspect(stashInfo.name)}`
			);
		}
		if(destTypeInWorkDir === FILE) {
			throw new PathAlreadyExistsError(
				`Cannot mv in working directory: refusing to overwrite ${inspect(dest)}` +
				` in working directory`
			);
		}
		if(destTypeInDb === DIRECTORY) {
			for(const dbPathSource of dbPathSources) {
				const parent = yield getUuidForPath(
					client, stashInfo.name, utils.getParentPath(dbPathSource));
				const row = yield getRowByParentBasename(
					client, stashInfo.name, parent, utils.getBaseName(dbPathSource), [utils.WILDCARD]);
				row.parent = yield getUuidForPath(client, stashInfo.name, dbPathDest);
				// row.basename is unchanged
				const cols = Object.keys(row);
				const qMarks = utils.filledArray(cols.length, "?");

				// This one checks the actual dir/basename instead of the dir/
				let actualDestTypeInDb = yield getTypeInDbByParentBasename(
					client, stashInfo.name, row.parent, row.basename);
				if(actualDestTypeInDb !== MISSING) {
					throw new PathAlreadyExistsError(
						`Cannot mv in database: destination parent=${row.parent.toString('hex')}` +
						` basename=${inspect(row.basename)} already exists in stash ${inspect(stashInfo.name)}`
					);
				}

				const actualDestInWorkDir = path.join(
					stashInfo.path, dbPathDest, utils.getBaseName(dbPathSource));
				const actualDestTypeInWorkDir = yield getTypeInWorkingDirectory(actualDestInWorkDir);
				if(actualDestTypeInWorkDir !== MISSING) {
					throw new PathAlreadyExistsError(
						`Cannot mv in working directory: refusing to overwrite` +
						` ${inspect(actualDestInWorkDir)}`
					);
				}

				yield runQuery(
					client,
					`INSERT INTO "${KEYSPACE_PREFIX + stashInfo.name}".fs
					(${utils.colsAsString(cols)})
					VALUES (${qMarks.join(", ")});`,
					cols.map(function(col) { return row[col]; })
				);

				yield runQuery(
					client,
					`DELETE FROM "${KEYSPACE_PREFIX + stashInfo.name}".fs
					WHERE parent = ? AND basename = ?;`,
					[parent, utils.getBaseName(dbPathSource)]
				);

				// Now move the file in the working directory
				yield mkdirpAsync(path.dirname(actualDestInWorkDir));
				const srcInWorkDir = path.join(stashInfo.path, dbPathSource);
				try {
					yield fs.renameAsync(srcInWorkDir, actualDestInWorkDir);
				} catch(err) {
					if(err.code !== "ENOENT") {
						throw err;
					}
					// It's okay if the file was missing in work dir
				}
			}
		} else {
			throw new Error("Haven't implemented mv to a non-dir dest yet");
		}

		/*else if(destTypeInDb === MISSING) {
			if(dbPathSources.length > 1) {

			}
		}*/
		//console.log({dbPathSources, dbPathDest, destTypeInDb});
	}));
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
			for(const row of result.rows) {
				const name = row.keyspace_name;
				if(name.startsWith(KEYSPACE_PREFIX)) {
					console.log(name.replace(KEYSPACE_PREFIX, ""));
				}
			}
		});
	});
}

const listChunkStores = Promise.coroutine(function* listChunkStores$coro() {
	const config = yield getChunkStores();
	for(const storeName of Object.keys(config.stores)) {
		console.log(storeName);
	}
});

const defineChunkStore = Promise.coroutine(function* defineChunkStores$coro(storeName, opts) {
	T(storeName, T.string, opts, T.object);
	const config = yield getChunkStores();
	if(utils.hasKey(config.stores, storeName)) {
		throw new Error(`${storeName} is already defined in chunk-stores.json`);
	}
	if(typeof opts.chunkSize !== "number") {
		throw new UsageError(`.chunkSize is missing or not a number on ${inspect(opts)}`);
	}
	const storeDef = {type: opts.type, chunkSize: opts.chunkSize};
	if(opts.type === "localfs") {
		if(typeof opts.directory !== "string") {
			throw new UsageError(`Chunk store type localfs requires a -d/--directory ` +
				`parameter with a string; got ${opts.directory}`
			);
		}
		storeDef.directory = opts.directory;
	} else if(opts.type === "gdrive") {
		if(typeof opts.clientId !== "string") {
			throw new UsageError(`Chunk store type gdrive requires a --client-id ` +
				`parameter with a string; got ${opts.clientId}`
			);
		}
		storeDef.clientId = opts.clientId;
		if(typeof opts.clientSecret !== "string") {
			throw new UsageError(`Chunk store type gdrive requires a --client-secret ` +
				`parameter with a string; got ${opts.clientSecret}`
			);
		}
		storeDef.clientSecret = opts.clientSecret;
	} else {
		throw new UsageError(`Type must be "localfs" or "gdrive" but was ${opts.type}`);
	}
	config.stores[storeName] = storeDef;
	yield utils.writeObjectToConfigFile("chunk-stores.json", config);
});

const configChunkStore = Promise.coroutine(function* configChunkStore$coro(storeName, opts) {
	T(storeName, T.string, opts, T.object);
	const config = yield getChunkStores();
	if(!utils.hasKey(config.stores, storeName)) {
		throw new Error(`${storeName} is not defined in chunk-stores.json`);
	}
	if(opts.type !== undefined) {
		T(opts.type, T.string);
		config.stores[storeName].type = opts.type;
	}
	if(opts.chunkSize !== undefined) {
		T(opts.chunkSize, T.number);
		config.stores[storeName].chunkSize = opts.chunkSize;
	}
	if(opts.directory !== undefined) {
		T(opts.directory, T.string);
		config.stores[storeName].directory = opts.directory;
	}
	if(opts.clientId !== undefined) {
		T(opts.clientId, T.string);
		config.stores[storeName].clientId = opts.clientId;
	}
	if(opts.clientSecret !== undefined) {
		T(opts.clientSecret, T.string);
		config.stores[storeName].clientSecret = opts.clientSecret;
	}
	yield utils.writeObjectToConfigFile("chunk-stores.json", config);
});

const questionAsync = function(question) {
	T(question, T.string);
	readline = loadNow(readline);
	return new Promise(function questionAsync$Promise(resolve) {
		const rl = readline.createInterface({
			input: process.stdin,
			output: process.stdout
		});
		rl.question(question, function(answer) {
			rl.close();
			resolve(answer);
		});
	});
};

const authorizeGDrive = Promise.coroutine(function* authorizeGDrive$coro(name) {
	T(name, T.string);
	gdrive = loadNow(gdrive);
	const config = yield getChunkStores();
	const chunkStore = config.stores[name];
	if(!(typeof chunkStore === "object" && chunkStore !== null)) {
		throw new Error(`Chunk store ${name} was ${chunkStore}, should be an object`);
	}
	if(!chunkStore.clientId) {
		throw new Error(`Chunk store ${name} is missing a clientId`);
	}
	if(!chunkStore.clientSecret) {
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
	const authCode = yield questionAsync("Authorization code: ");
	console.log("OK, sending the authorization code to Google to get a refresh token...");
	yield gdriver.importAuthCode(authCode);
	console.log("OK, saved the refresh token and access token.");
});

function assertName(name) {
	T(name, T.string);
	A(name, "Name must not be empty");
}

const destroyStash = Promise.coroutine(function* destroyStash$coro(stashName) {
	assertName(stashName);
	yield doWithClient(getNewClient(), function destroyStash$doWithClient(client) {
		return runQuery(
			client,
			`DROP KEYSPACE "${KEYSPACE_PREFIX + stashName}";`
		);
	});
	const config = yield getStashes();
	utils.deleteKey(config.stashes, stashName);
	yield utils.writeObjectToConfigFile("stashes.json", config);
	console.log(`Destroyed keyspace and removed config for ${stashName}.`);
});

/**
 * Initialize a new stash
 */
const initStash = Promise.coroutine(function* initStash$coro(stashPath, stashName, options) {
	T(
		stashPath, T.string,
		stashName, T.string,
		options, T.shape({
			chunkStore: T.string,
			chunkThreshold: T.number
		})
	);
	assertName(stashName);

	let caught;
	try {
		yield getStashInfoByPath(stashPath);
	} catch(err) {
		if(!(err instanceof NotInWorkingDirectoryError)) {
			throw err;
		}
		caught = true;
	}
	if(!caught) {
		throw new Error(`${stashPath} is already configured as a stash`);
	}

	return doWithClient(getNewClient(), Promise.coroutine(function* initStash$coro$coro(client) {
		yield runQuery(client, `CREATE KEYSPACE IF NOT EXISTS "${KEYSPACE_PREFIX + stashName}"
			WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`);

		// An individual chunk
		yield runQuery(client, `CREATE TYPE "${KEYSPACE_PREFIX + stashName}".chunk (
			idx int,
			file_id text,
			md5 blob,
			crc32c blob,
			size bigint,
			version int,
			block_size int
		)`);

		yield runQuery(client, `CREATE TABLE IF NOT EXISTS "${KEYSPACE_PREFIX + stashName}".fs (
			basename text,
			type ascii,
			parent blob,
			uuid blob,
			size bigint,
			content blob,
			crc32c blob,
			key blob,
			mtime timestamp,
			executable boolean,
			PRIMARY KEY (parent, basename)
		);`);
		// The above PRIMARY KEY lets us select on both parent and (parent, basename)

		// Note: chunks_in_* columns are added by defineChunkStore.
		// We use column-per-chunk-store instead of having a map of
		// <chunkStore, chunkInfo> because non-frozen, nested collections
		// aren't implemented: https://issues.apache.org/jira/browse/CASSANDRA-7826

		const config = yield getStashes();
		config.stashes[stashName] = {
			path: path.resolve(stashPath),
			chunkStore: options.chunkStore,
			chunkThreshold: options.chunkThreshold
		};
		yield utils.writeObjectToConfigFile("stashes.json", config);
	}));
});

let transitWriter;
function getTransitWriter() {
	cassandra = loadNow(cassandra);
	transit = loadNow(transit);
	if(!transitWriter) {
		transitWriter = transit.writer("json-verbose", {handlers: transit.map([
			cassandra.types.Row,
			transit.makeWriteHandler({
				tag: function(v, h) { return "Row"; },
				rep: function(v, h) { return Object.assign({}, v); }
			}),
			cassandra.types.Long,
			transit.makeWriteHandler({
				tag: function(v, h) { return "Long"; },
				rep: function(v, h) { return String(v); }
			})
		])});
	}
	return transitWriter;
}

let transitReader;
function getTransitReader() {
	cassandra = loadNow(cassandra);
	transit = loadNow(transit);
	if(!transitReader) {
		transitReader = transit.reader("json-verbose", {handlers: {
			"Long": function(v) { return new cassandra.types.Long(v); },
			"Row": function(v) {
				const obj = transit.mapToObject(v);
				// We also need to fix the TransitMap objects inside
				// chunks_in_* -> [TransitMap, ...]
				for(const k of Object.keys(obj)) {
					if(k.startsWith("chunks_in_") && obj[k] !== null) {
						obj[k] = obj[k].map(function(v) {
							return transit.mapToObject(v);
						});
					}
				}
				return obj;
			}
		}});
	}
	return transitReader;
}

class RowToTransit extends Transform {
	constructor(options) {
		super(options);
		this.transitWriter = getTransitWriter();
	}

	_transform(row, encoding, callback) {
		let s;
		try {
			s = this.transitWriter.write(row) + "\n";
		} catch(err) {
			callback(err);
			return;
		}
		callback(null, s);
	}
}

function exportDb(stashName) {
	T(stashName, T.maybe(T.string));
	return doWithClient(getNewClient(), function exportDb$doWithClient(client) {
		return doWithPath(stashName, ".", Promise.coroutine(function* exportDb$coro(stashInfo, dbPath, parentPath) {
			T(stashInfo.name, T.string);
			yield new Promise(function(resolve, reject) {
				const rowStream = client.stream(
					`SELECT * FROM "${KEYSPACE_PREFIX + stashInfo.name}".fs;`, [], {autoPage: true, prepare: true});
				const transitStream = new RowToTransit({objectMode: true});
				utils.pipeWithErrors(rowStream, transitStream);
				utils.pipeWithErrors(transitStream, process.stdout);
				transitStream.once('end', resolve);
				transitStream.once('error', reject);
			});
		}));
	});
}

class TransitToInsert extends Transform {
	constructor(client, stashName) {
		T(client, CassandraClientType, stashName, T.string);
		super({readableObjectMode: true});
		this._client = client;
		this._stashName = stashName;
		this._transitReader = getTransitReader();
		this._columnsCreated = {};
		sse4_crc32 = loadNow(sse4_crc32);
		hasher = loadNow(hasher);
	}

	*_insertFromLine(lineBuf) {
		const line = lineBuf.toString('utf-8');
		const obj = this._transitReader.read(line);

		if(obj.crc32c === undefined) {
			obj.crc32c = null;
		}
		if(obj.content === undefined) {
			obj.content = null;
		}

		T(obj.basename, T.string);
		T(obj.type, T.string);
		T(obj.mtime, Date);
		T(obj.parent, Buffer);
		A.eq(obj.parent.length, 128/8);
		if(obj.type === 'f') {
			if(obj.crc32c === null) {
				if(obj.content !== null) {
					// Generate crc32c for version 1 dumps, which have blake2b224
					// instead of crc32c.
					T(obj.content, Buffer);
					obj.crc32c = hasher.crcToBuf(sse4_crc32.calculate(obj.content));
				}
			} else {
				T(obj.crc32c, Buffer);
			}
			if(obj.content === null) {
				T(obj.key, Buffer);
				A.eq(obj.key.length, 128/8);
			}
			T(obj.size, cassandra.types.Long);
			A.eq(obj.uuid, null);
			T(obj.executable, T.boolean);
		} else if(obj.type === 'd') {
			T(obj.uuid, Buffer);
			A.eq(obj.uuid.length, 128/8);
			A.eq(obj.content, null);
			A.eq(obj.executable, null);
			A.eq(obj.crc32c, null);
			A.eq(obj.size, null);
		}

		const cols = ['basename', 'parent', 'type', 'uuid', 'content', 'key', 'size', 'crc32c', 'mtime', 'executable'];
		const vals = [obj.basename, obj.parent, obj.type, obj.uuid, obj.content, obj.key, obj.size, obj.crc32c, obj.mtime, obj.executable];
		for(const k of Object.keys(obj)) {
			if(k.startsWith("chunks_in_")) {
				if(!this._columnsCreated[k]) {
					yield tryCreateColumnOnStashTable(
						this._client, this._stashName, k, 'list<frozen<chunk>>');
					this._columnsCreated[k] = true;
				}

				if(obj[k] !== null) {
					for(const chunkInfo of obj[k]) {
						if(chunkInfo.version === undefined) {
							chunkInfo.version = 2;
							chunkInfo.block_size = 0;
						}
					}
				}

				cols.push(k);
				vals.push(obj[k]);
			}
		}
		const qMarks = utils.filledArray(cols.length, "?");

		const query = `INSERT INTO "${KEYSPACE_PREFIX + this._stashName}".fs
			(${utils.colsAsString(cols)})
			VALUES (${qMarks.join(", ")});`;
		//console.log({query, cols, vals});
		yield runQuery(this._client, query, vals);
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
TransitToInsert.prototype._insertFromLine = Promise.coroutine(TransitToInsert.prototype._insertFromLine);

function importDb(outCtx, stashName, dumpFile) {
	T(outCtx, OutputContextType, stashName, T.string, dumpFile, T.string);
	if(outCtx.mode !== 'quiet') {
		console.log(`Restoring from ${dumpFile === '-' ? 'stdin' : inspect(dumpFile)} into stash ${inspect(stashName)}.`);
		console.log('Note that files may be restored before directories, so you might ' +
			'not see anything in the stash until the restore process is complete.');
	}
	return doWithClient(getNewClient(), Promise.coroutine(function* importDb$coro(client) {
		let inputStream;
		if(dumpFile === '-') {
			inputStream = process.stdin;
		} else {
			inputStream = fs.createReadStream(dumpFile);
		}
		line_reader = loadNow(line_reader);
		work_stealer = loadNow(work_stealer);
		const lineStream = new line_reader.DelimitedBufferDecoder(new Buffer("\n"));
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

			inserter.on('data', function(obj) {
				count += 1;
				// Print every 100th to avoid getting 30% slowdown by just terminal output
				if(outCtx.mode === 'terminal' && count % 100 === 0) {
					printProgress();
				} else if(outCtx.mode === 'log' && count % 1000 === 0) {
					printProgress();
				}
			});

			return new Promise(function(resolve, reject) {
				inserter.once('end', resolve);
				inserter.once('error', reject);
			});
		});
		yield Promise.all(inserters);
		if(outCtx.mode !== 'quiet') {
			printProgress();
			console.log('\nDone importing.');
		}
	}));
}

module.exports = {
	initStash, destroyStash, getStashes, getChunkStores, authorizeGDrive,
	listTerastashKeyspaces, listChunkStores, defineChunkStore, configChunkStore,
	addFile, addFiles, getFile, getFiles, catFile, catFiles, dropFile, dropFiles,
	shooFile, shooFiles, moveFiles, makeDirectories, lsPath, findPath,
	KEYSPACE_PREFIX, exportDb, importDb,
	DirectoryNotEmptyError, NotInWorkingDirectoryError, NoSuchPathError,
	NotAFileError, PathAlreadyExistsError, KeyspaceMissingError,
	DifferentStashesError, UnexpectedFileError, UsageError, FileChangedError,
	CRC_BLOCK_SIZE, checkChunkSize
};
