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
const commaify = utils.numberWithCommas;
const compile_require = require('./compile_require');
const RetryPolicy = require('cassandra-driver/lib/policies/retry').RetryPolicy;
let cassandra;
let localfs;
let blake2;
let gdrive;
let readline;
let padded_stream;
let transit;
let objectAssign = Object.assign;

const KEYSPACE_PREFIX = "ts_";

// TODO: get rid of this, use streamifier
function blake2b224Buffer(buf) {
	T(buf, Buffer);
	if(!blake2) {
		blake2 = compile_require('blake2');
	}
	return blake2.createHash('blake2b').update(buf).digest().slice(0, 224/8);
}

let CassandraClientType = T.object;

function loadCassandra() {
	cassandra = require('cassandra-driver');
	CassandraClientType = cassandra.Client;
}

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
	if(!cassandra) {
		loadCassandra();
	}
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
function doWithClient(f) {
	T(f, T.function);
	const client = getNewClient();
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

	const row = yield getRowByParentBasename(client, stashName, parent, basename, ['uuid']);
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
		${limit === undefined ? "" : "LIMIT " + limit}`,
		[parent], {autoPage: true, prepare: true}
	);
	rowStream.on('readable', function() {
		let row;
		while(row = this.read()) {
			rows.push(row);
		}
	});
	return new Promise(function(resolve, reject) {
		rowStream.on('end', function() { resolve(rows); });
		rowStream.once('error', reject);
	});
});

function lsPath(stashName, options, p) {
	return doWithClient(function lsPath$doWithClient(client) {
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

const iv0 = new Buffer('00000000000000000000000000000000', 'hex');
A.eq(iv0.length, 128/8);

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
selfTests = {aes: function() {
	require('./aes').selfTest();
	selfTests.aes = noop;
}};

/**
 * Put file `p` into the Cassandra database as path `dbPath`.
 */
const addFile = Promise.coroutine(function*(client, stashInfo, p, dbPath) {
	checkDbPath(dbPath);
	const parentPath = utils.getParentPath(dbPath);

	if(parentPath) {
		yield makeDirsInDb(client, stashInfo.name, path.dirname(p), parentPath);
	}

	const parentUuid = yield getUuidForPath(client, stashInfo.name, parentPath);
	const throwIfAlreadyInDb = Promise.coroutine(function*() {
		const typeInDb = yield getTypeInDbByPath(client, stashInfo.name, dbPath);
		if(typeInDb !== MISSING) {
			throw new PathAlreadyExistsError(
				`Cannot add to database:` +
				` ${inspect(dbPath)} in stash ${inspect(stashInfo.name)}` +
				` already exists as a ${typeInDb === DIRECTORY ? "directory" : "file"}`);
		}
	});

	// Check early to avoid uploading to chunk store and doing other work
	yield throwIfAlreadyInDb();

	const type = 'f';
	const stat = yield fs.statAsync(p);
	const mtime = stat.mtime;
	const executable = Boolean(stat.mode & 0o100); /* S_IXUSR */
	const sticky = Boolean(stat.mode & 0o1000);
	if(sticky) {
		throw new UnexpectedFileError(
			`Refusing to add file ${inspect(p)} because it has sticky bit set,` +
			` which may have been set by 'ts shoo'`
		);
	}
	const chunkStore = yield getChunkStore(stashInfo);
	let blake2b224;
	let content = null;
	let chunkInfo;
	let size;
	let key = null;

	if(stat.size >= stashInfo.chunkThreshold) {
		// TODO: validate storeName
		key = makeKey();

		const inputStream = fs.createReadStream(p);
		if(!padded_stream) {
			padded_stream = require('./padded_stream');
		}
		const hasher = utils.streamHasher(inputStream, 'blake2b');
		const concealedSize = utils.concealSize(stat.size);
		const padder = new padded_stream.Padder(concealedSize);
		utils.pipeWithErrors(hasher.stream, padder);
		selfTests.aes();
		const cipherStream = crypto.createCipheriv('aes-128-ctr', key, iv0);
		utils.pipeWithErrors(padder, cipherStream);

		let _;
		if(chunkStore.type === "localfs") {
			if(!localfs) {
				localfs = require('./chunker/localfs');
			}
			_ = yield localfs.writeChunks(chunkStore.directory, cipherStream, chunkStore.chunkSize);
		} else if(chunkStore.type === "gdrive") {
			if(!gdrive) {
				gdrive = require('./chunker/gdrive');
			}
			const gdriver = new gdrive.GDriver(chunkStore.clientId, chunkStore.clientSecret);
			yield gdriver.loadCredentials();
			_ = yield gdrive.writeChunks(gdriver, chunkStore.parents, cipherStream, chunkStore.chunkSize);
		} else {
			throw new Error(`Unknown chunk store type ${inspect(chunkStore.type)}`);
		}

		const totalSize = _[0];
		chunkInfo = _[1];
		A.eq(padder.bytesRead, stat.size,
			`For ${dbPath}, read\n` +
			`${commaify(padder.bytesRead)} bytes instead of the expected\n` +
			`${commaify(stat.size)} bytes; did file change during reading?`);
		A.eq(totalSize, concealedSize,
			`For ${dbPath}, wrote to chunks\n` +
			`${commaify(totalSize)} bytes instead of the expected\n` +
			`${commaify(concealedSize)} (concealed) bytes`);
		T(chunkInfo, Array);

		blake2b224 = hasher.hash.digest().slice(0, 224/8);
		size = stat.size;
	} else {
		content = yield fs.readFileAsync(p);
		blake2b224 = blake2b224Buffer(content);
		size = content.length;
		A.eq(size, stat.size,
			`For ${dbPath}, read\n` +
			`${commaify(size)} bytes instead of the expected\n` +
			`${commaify(stat.size)} bytes; did file change during reading?`);
	}

	function insert() {
		return runQuery(
			client,
			`INSERT INTO "${KEYSPACE_PREFIX + stashInfo.name}".fs
			(basename, parent, type, content, key, "chunks_in_${chunkStore.name}", size, blake2b224, mtime, executable)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
			[utils.getBaseName(dbPath), parentUuid, type, content, key, chunkInfo, size, blake2b224, mtime, executable]
		);
	}

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

/**
 * Put files or directories into the Cassandra database.
 */
function addFiles(paths, skipExisting, replaceExisting, progress) {
	T(
		paths, T.list(T.string),
		skipExisting, T.optional(T.boolean),
		replaceExisting, T.optional(T.boolean),
		progress, T.optional(T.boolean)
	);
	return doWithClient(Promise.coroutine(function* addFiles$coro(client) {
		if(skipExisting && replaceExisting) {
			throw new UsageError("skipExisting and replaceExisting are mutually exclusive");
		}

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
				if(progress) {
					process.stdout.clearLine();
					process.stdout.cursorTo(0);
					process.stdout.write(`${count}/${paths.length}...`);
				}
				const dbPath = userPathToDatabasePath(stashInfo.path, p);
				try {
					yield addFile(client, stashInfo, p, dbPath);
				} catch(err) {
					if(!(err instanceof PathAlreadyExistsError)) {
						throw err;
					}
					if(skipExisting) {
						console.error(chalk.red(err.message));
					} else if(replaceExisting) {
						yield dropFile(client, stashInfo.name, p);
						yield addFile(client, stashInfo, p, dbPath);
					} else {
						throw err;
					}
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

const StashInfoType = T.shape({path: T.string});

const makeEmptySparseFile = Promise.coroutine(function* makeEmptySparseFile$coro(p, size) {
	T(p, T.string, size, T.number);
	const handle = yield fs.openAsync(p, "w");
	try {
		yield fs.truncateAsync(handle, size);
	} finally {
		yield fs.closeAsync(handle);
	}
});

const shooFile = Promise.coroutine(function* shooFile$coro(client, stashInfo, p) {
	T(client, CassandraClientType, stashInfo, StashInfoType, p, T.string);
	const dbPath = userPathToDatabasePath(stashInfo.path, p);
	const row = yield getRowByPath(client, stashInfo.name, dbPath, ['mtime', 'size', 'type']);
	if(row.type === 'd') {
		throw new NotAFileError(`Can't put away dbPath=${inspect(dbPath)}; it is a directory`);
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
		yield makeEmptySparseFile(p, stat.size);
		// Set the mtime because the truncate() in makeEmptySparseFile reset it
		yield utils.utimesMilliseconds(p, row.mtime, row.mtime);
		const withSticky = stat.mode | 0o1000;
		yield fs.chmodAsync(p, withSticky);
	} else {
		throw new Error(`Unexpected type ${inspect(row.type)} for dbPath=${inspect(dbPath)}`);
	}
});

function shooFiles(paths) {
	T(paths, T.list(T.string));
	return doWithClient(Promise.coroutine(function* shooFiles$coro(client) {
		const stashInfo = yield getStashInfoForPaths(paths);
		for(const p of paths) {
			yield shooFile(client, stashInfo, p);
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
			["size", "type", "key", `chunks_in_${storeName}`, "blake2b224", "content", "mtime", "executable"]
		);
	} catch(err) {
		if(!isColumnMissingError(err)) {
			throw err;
		}
		// chunks_in_${storeName} doesn't exist, try the query without it
		row = yield getRowByPath(client, stashInfo.name, dbPath,
			["size", "type", "key", "blake2b224", "content", "mtime", "executable"]
		);
	}
	if(row.type !== 'f') {
		throw new NotAFileError(`Path ${inspect(dbPath)} in stash ${inspect(stashInfo.name)} is not a file`);
	}

	const chunkStore = (yield getChunkStores()).stores[storeName];
	const chunks = row[`chunks_in_${storeName}`];
	let hasher;
	if(chunks !== null) {
		validateChunks(chunks);
		A.eq(row.content, null);
		A.eq(row.key.length, 128/8);
		let cipherStream;
		if(chunkStore.type === "localfs") {
			if(!localfs) {
				localfs = require('./chunker/localfs');
			}
			const chunksDir = chunkStore.directory;
			cipherStream = localfs.readChunks(chunksDir, chunks);
		} else if(chunkStore.type === "gdrive") {
			if(!gdrive) {
				gdrive = require('./chunker/gdrive');
			}
			const gdriver = new gdrive.GDriver(chunkStore.clientId, chunkStore.clientSecret);
			yield gdriver.loadCredentials();
			cipherStream = gdrive.readChunks(gdriver, chunks);
		} else {
			throw new Error(`Unknown chunk store type ${inspect(chunkStore.type)}`);
		}
		selfTests.aes();
		const clearStream = crypto.createCipheriv('aes-128-ctr', row.key, iv0);
		utils.pipeWithErrors(cipherStream, clearStream);
		if(!padded_stream) {
			padded_stream = require('./padded_stream');
		}
		const unpadder = new padded_stream.Unpadder(Number(row.size));
		utils.pipeWithErrors(clearStream, unpadder);
		hasher = utils.streamHasher(unpadder, 'blake2b');
	} else {
		const streamWrapper = streamifier.createReadStream(row.content);
		hasher = utils.streamHasher(streamWrapper, 'blake2b');
	}
	hasher.stream.once('end', function() {
		if(hasher.length !== Number(row.size)) {
			hasher.stream.emit('error', new Error(
				`For dbPath=${dbPath}, expected length of content to be\n` +
				`${commaify(row.size)} but was\n` +
				`${commaify(hasher.length)}`
			));
		}
		const digest = hasher.hash.digest().slice(0, 224/8);
		if(!digest.equals(row.blake2b224)) {
			hasher.stream.emit('error', new Error(
				`For dbPath=${dbPath}, expected blake2b224 of content to be\n` +
				`${row.blake2b224.toString('hex')} but was\n` +
				`${digest.toString('hex')}`
			));
		}
	});
	return [row, hasher.stream];
});

/**
 * Get a file or directory from the Cassandra database.
 */
function getFile(client, stashName, p) {
	return doWithPath(stashName, p, Promise.coroutine(function* getFile$coro(stashInfo, dbPath, parentPath) {
		const _ = yield streamFile(client, stashInfo, dbPath);
		const row = _[0];
		const readStream = _[1];

		let outputFilename;
		// If stashName was given, write file to current directory
		if(stashName) {
			outputFilename = utils.getBaseName(dbPath);
		} else {
			T(stashInfo.path, T.string);
			outputFilename = stashInfo.path + '/' + dbPath;
		}

		yield mkdirpAsync(path.dirname(outputFilename));

		// Delete the existing file because it may have the sticky bit set
		// or other unwanted permissions.
		yield utils.tryUnlink(outputFilename);

		const writeStream = fs.createWriteStream(outputFilename);
		utils.pipeWithErrors(readStream, writeStream);
		yield new Promise(function getFiles$Promise(resolve, reject) {
			writeStream.once('finish', Promise.coroutine(function*() {
				resolve();
			}));
			writeStream.once('error', function(err) {
				reject(err);
			});
			readStream.once('error', function(err) {
				reject(err);
			});
		});
		yield utils.utimesMilliseconds(outputFilename, row.mtime, row.mtime);
		if(row.executable) {
			// TODO: setting for 0o700 instead?
			yield fs.chmodAsync(outputFilename, 0o770);
		}
	}));
}

function getFiles(stashName, paths) {
	return doWithClient(Promise.coroutine(function* getFiles$coro(client) {
		for(const p of paths) {
			yield getFile(client, stashName, p);
		}
	}));
}

function catFile(client, stashName, p) {
	return doWithPath(stashName, p, Promise.coroutine(function* catFile$coro(stashInfo, dbPath, parentPath) {
		const _ = yield streamFile(client, stashInfo, dbPath);
		//const row = _[0];
		const readStream = _[1];
		utils.pipeWithErrors(readStream, process.stdout);
	}));
}

function catFiles(stashName, paths) {
	return doWithClient(Promise.coroutine(function* catFiles$coro(client) {
		for(const p of paths) {
			yield catFile(client, stashName, p);
		}
	}));
}

function dropFile(client, stashName, p) {
	return doWithPath(stashName, p, Promise.coroutine(function* doWithPath$coro(stashInfo, dbPath, parentPath) {
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
				if(!localfs) {
					localfs = require('./chunker/localfs');
				}
				yield localfs.deleteChunks(chunkStore.directory, chunks);
			} else {
				if(!gdrive) {
					gdrive = require('./chunker/gdrive');
				}
				const gdriver = new gdrive.GDriver(chunkStore.clientId, chunkStore.clientSecret);
				yield gdriver.loadCredentials();
				yield gdrive.deleteChunks(gdriver, chunks);
			}
		}
	}));
}

/**
 * Remove files from the Cassandra database and their corresponding chunks.
 */
function dropFiles(stashName, paths) {
	return doWithClient(Promise.coroutine(function* dropFiles$coro(client) {
		for(const p of paths) {
			yield dropFile(client, stashName, p);
		}
	}));
}

function makeDirectories(stashName, paths) {
	T(stashName, T.maybe(T.string), paths, T.list(T.string));
	return doWithClient(Promise.coroutine(function* makeDirectories$coro(client) {
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
	return doWithClient(Promise.coroutine(function* moveFiles$coro(client) {
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
	return doWithClient(function listTerastashKeyspaces$doWithClient(client) {
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
	if(!readline) { readline = require('readline'); }
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
	if(!gdrive) { gdrive = require('./chunker/gdrive'); }
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
	yield doWithClient(function destroyStash$doWithClient(client) {
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

	return doWithClient(Promise.coroutine(function* initStash$coro$coro(client) {
		yield runQuery(client, `CREATE KEYSPACE IF NOT EXISTS "${KEYSPACE_PREFIX + stashName}"
			WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`);

		// An individual chunk
		yield runQuery(client, `CREATE TYPE "${KEYSPACE_PREFIX + stashName}".chunk (
			idx int,
			file_id text,
			md5 blob,
			crc32c blob,
			size bigint
		)`);

		yield runQuery(client, `CREATE TABLE IF NOT EXISTS "${KEYSPACE_PREFIX + stashName}".fs (
			basename text,
			type ascii,
			parent blob,
			uuid blob,
			size bigint,
			content blob,
			blake2b224 blob,
			key blob,
			mtime timestamp,
			crtime timestamp,
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
	if(!cassandra) {
		loadCassandra();
	}
	if(!transit) {
		transit = require('transit-js');
	}
	if(!objectAssign) {
		objectAssign = require('object-assign');
	}
	if(!transitWriter) {
		transitWriter = transit.writer("json-verbose", {handlers: transit.map([
			cassandra.types.Row,
			transit.makeWriteHandler({
				tag: function(v, h) { return "Row"; },
				rep: function(v, h) { return objectAssign({}, v); }
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

function dumpDb(stashName) {
	T(stashName, T.maybe(T.string));
	return doWithClient(function dumpDb$doWithClient(client) {
		return doWithPath(stashName, ".", Promise.coroutine(function* dumpDb$coro(stashInfo, dbPath, parentPath) {
			T(stashInfo.name, T.string);
			yield new Promise(function(resolve, reject) {
				const rowStream = client.stream(
					`SELECT * FROM "${KEYSPACE_PREFIX + stashInfo.name}".fs;`, [], {autoPage: true, prepare: true});
				const transitStream = new RowToTransit({objectMode: true});
				utils.pipeWithErrors(rowStream, transitStream);
				utils.pipeWithErrors(transitStream, process.stdout);
				transitStream.on('end', resolve);
				transitStream.once('error', reject);
			});
		}));
	});
}

module.exports = {
	initStash, destroyStash, getStashes, getChunkStores, authorizeGDrive,
	listTerastashKeyspaces, listChunkStores, defineChunkStore, configChunkStore,
	addFile, addFiles, getFile, getFiles, catFile, catFiles, dropFile, dropFiles,
	shooFile, shooFiles, moveFiles, makeDirectories, lsPath, KEYSPACE_PREFIX, dumpDb,
	DirectoryNotEmptyError, NotInWorkingDirectoryError, NoSuchPathError,
	NotAFileError, PathAlreadyExistsError, KeyspaceMissingError,
	DifferentStashesError, UnexpectedFileError, UsageError
};
