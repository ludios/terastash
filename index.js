"use strong";
"use strict";

const A = require('ayy');
const T = require('notmytype');
const Promise = require('bluebird');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const chalk = require('chalk');
const inspect = require('util').inspect;
const streamifier = require('streamifier');

const utils = require('./utils');
const compile_require = require('./compile_require');
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

function getNewClient() {
	if(!cassandra) {
		loadCassandra();
	}
	return new cassandra.Client({contactPoints: ['localhost']});
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
		_comment: utils.ol(`You cannot change the name of a store because existing
			chunks reference it by name in the Cassandra database.`)
	}
);

/**
 * For a given pathname, return a stash that contains the file,
 * or `null` if there is no terastash base.
 */
const getStashInfoByPath = Promise.coroutine(function*(pathname) {
	T(pathname, T.string);
	const config = yield getStashes();
	if(!config.stashes || typeof config.stashes !== "object") {
		throw new Error(`terastash config has no "stashes" or not an object`);
	}

	const resolvedPathname = path.resolve(pathname);
	for(const stashName of Object.keys(config.stashes)) {
		const stash = config.stashes[stashName];
		//console.log(resolvedPathname, stash.path);
		if(resolvedPathname === stash.path || resolvedPathname.startsWith(stash.path + '/')) {
			stash.name = stashName;
			return stash;
		}
	}
	return null;
});

/**
 * Return a stash for a given stash name
 */
const getStashInfoByName = Promise.coroutine(function*(stashName) {
	T(stashName, T.string);
	const config = yield getStashes();
	if(!config.stashes || typeof config.stashes !== "object") {
		throw new Error(`terastash config has no "stashes" or not an object`);
	}

	const stash = config.stashes[stashName];
	if(!stash) {
		return null;
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
 * Run a Cassandra query and return a Promise that is fulfilled
 * with the query results.
 */
function runQuery(client, statement, args) {
	T(client, CassandraClientType, statement, T.string, args, T.optional(Array));
	//console.log(`runQuery(${client}, ${inspect(statement)}, ${inspect(args)})`);
	return new Promise(function(resolve, reject) {
		client.execute(statement, args, {prepare: true}, function(err, result) {
			if(err) {
				reject(err);
			} else {
				resolve(result);
			}
		});
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

const doWithPath = Promise.coroutine(function*(stashName, p, fn) {
	T(stashName, T.maybe(T.string), p, T.string, fn, T.function);
	const resolvedPathname = path.resolve(p);
	let dbPath;
	let stashInfo;
	if(stashName) { // Explicit stash name provided
		stashInfo = yield getStashInfoByName(stashName);
		if(!stashInfo) {
			throw new Error(`No stash with name ${stashName}; consult terastash.json and ts help`);
		}
		dbPath = p;
	} else {
		stashInfo = yield getStashInfoByPath(resolvedPathname);
		if(!stashInfo) {
			throw new Error(`File ${p} is not in a stash directory; consult terastash.json and ts help`);
		}
		dbPath = userPathToDatabasePath(stashInfo.path, p);
	}

	const parentPath = utils.getParentPath(dbPath);
	A(!parentPath.startsWith('/'), parentPath);

	// TODO: validate stashInfo.name - it may contain injection
	return fn(stashInfo, dbPath, parentPath);
});

const pathnameSorterAsc = utils.comparedBy(function(row) {
	return utils.getBaseName(row.pathname);
});

const pathnameSorterDesc = utils.comparedBy(function(row) {
	return utils.getBaseName(row.pathname);
}, true);

const mtimeSorterAsc = utils.comparedBy(function(row) {
	return row.mtime;
});

const mtimeSorterDesc = utils.comparedBy(function(row) {
	return row.mtime;
}, true);

function lsPath(stashName, options, p) {
	return doWithClient(function(client) {
		return doWithPath(stashName, p, function(stashInfo, dbPath, parentPath) {
			return runQuery(
				client,
				`SELECT pathname, type, size, mtime, executable
				from "${KEYSPACE_PREFIX + stashInfo.name}".fs
				WHERE parent = ?`,
				[dbPath]
			).then(function(result) {
				if(options.sortByMtime) {
					result.rows.sort(options.reverse ? mtimeSorterAsc : mtimeSorterDesc);
				} else {
					result.rows.sort(options.reverse ? pathnameSorterDesc : pathnameSorterAsc);
				}
				for(const row of result.rows) {
					const baseName = utils.getBaseName(row.pathname);
					if(options.justNames) {
						console.log(baseName);
					} else {
						let decoratedName = baseName;
						if(row.type === 'd') {
							decoratedName = chalk.bold.blue(decoratedName);
							decoratedName += '/';
						} else if(row.executable) {
							decoratedName = chalk.bold.green(decoratedName);
							decoratedName += '*';
						}
						console.log(
							utils.pad(utils.numberWithCommas((row.size || 0).toString()), 18) + " " +
							utils.shortISO(row.mtime) + " " +
							decoratedName
						);
					}
				}
			});
		});
	});
}

const makeDirs = Promise.coroutine(function*(client, stashInfo, p, dbPath) {
	const type = 'd';
	const stat = yield utils.statAsync(p);
	const mtime = stat.mtime;
	const parentPath = utils.getParentPath(dbPath);
	if(parentPath) {
		yield makeDirs(client, stashInfo, p, parentPath);
	}
	yield runQuery(
		client,
		`INSERT INTO "${KEYSPACE_PREFIX + stashInfo.name}".fs
		(pathname, parent, type, mtime) VALUES (?, ?, ?, ?);`,
		[dbPath, parentPath, type, mtime]
	);
});

const tryCreateColumnOnStashTable = Promise.coroutine(function*(client, stashName, columnName, type) {
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

let keyCounterForTests = 0;
function makeKey() {
	if(Number(process.env.TERASTASH_INSECURE_AND_DETERMINISTIC)) {
		const buf = new Buffer(128/8).fill(0);
		buf.writeIntBE(keyCounterForTests, 0, 128/8);
		keyCounterForTests += 1;
		return buf;
	} else {
		return crypto.randomBytes(128/8);
	}
}

/**
 * Put a file or directory into the Cassandra database.
 */
function putFile(client, p) {
	return doWithPath(null, p, Promise.coroutine(function*(stashInfo, dbPath, parentPath) {
		const type = 'f';
		const stat = yield utils.statAsync(p);
		const mtime = stat.mtime;
		const executable = Boolean(stat.mode & 0o100); /* S_IXUSR */
		const storeName = stashInfo.chunkStore;
		if(!storeName) {
			throw new Error("stash info doesn't specify chunkStore key");
		}

		if(parentPath) {
			yield makeDirs(client, stashInfo, path.dirname(p), parentPath);
		}

		if(stat.size >= stashInfo.chunkThreshold) {
			// TODO: validate storeName
			// TODO: do this query only if we fail to add a file
			yield tryCreateColumnOnStashTable(
				client, stashInfo.name, `chunks_in_${storeName}`, 'list<frozen<chunk>>');
			const key = makeKey();
			const config = yield getChunkStores();
			const chunkStore = config.stores[storeName];
			if(!chunkStore) {
				throw new Error(`Chunk store ${storeName} is not defined in chunk-stores.json`);
			}

			const inputStream = fs.createReadStream(p);
			if(!padded_stream) {
				padded_stream = require('./padded_stream');
			}
			const hasher = utils.streamHasher(inputStream, 'blake2b');
			const concealedSize = utils.concealSize(stat.size);
			const padder = new padded_stream.Padder(concealedSize);
			hasher.stream.pipe(padder);
			const cipherStream = crypto.createCipheriv('aes-128-ctr', key, iv0);
			padder.pipe(cipherStream);

			let _;
			if(chunkStore.type === "localfs") {
				if(!localfs) {
					localfs = require('./chunker/localfs');
				}
				_ = yield localfs.writeChunks(chunkStore.directory, cipherStream, chunkStore.chunkSize);
			} else {
				if(!gdrive) {
					gdrive = require('./chunker/gdrive');
				}
				const gdriver = new gdrive.GDriver(chunkStore.clientId, chunkStore.clientSecret);
				yield gdriver.loadCredentials();
				_ = yield gdrive.writeChunks(gdriver, chunkStore.parents, cipherStream, chunkStore.chunkSize);
			}

			const totalSize = _[0];
			const chunkInfo = _[1];
			A.eq(padder.bytesRead, stat.size,
				`For ${dbPath}, read\n` +
				`${utils.numberWithCommas(padder.bytesRead)} bytes instead of the expected\n` +
				`${utils.numberWithCommas(stat.size)} bytes; did file change during reading?`);
			A.eq(totalSize, concealedSize,
				`For ${dbPath}, wrote to chunks\n` +
				`${utils.numberWithCommas(totalSize)} bytes instead of the expected\n` +
				`${utils.numberWithCommas(concealedSize)} (concealed) bytes`);
			T(chunkInfo, Array);

			const blake2b224 = hasher.hash.digest().slice(0, 224/8);
			// TODO: make sure file does not already exist? require additional flag to update?
			yield runQuery(
				client,
				`INSERT INTO "${KEYSPACE_PREFIX + stashInfo.name}".fs
				(pathname, parent, type, key, "chunks_in_${storeName}", size, blake2b224, mtime, executable)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);`,
				[dbPath, parentPath, type, key, chunkInfo, stat.size, blake2b224, mtime, executable]
			);
		} else {
			const content = yield utils.readFileAsync(p);
			const blake2b224 = blake2b224Buffer(content);
			const size = content.length;
			A.eq(size, stat.size,
				`For ${dbPath}, read\n` +
				`${utils.numberWithCommas(size)} bytes instead of the expected\n` +
				`${utils.numberWithCommas(stat.size)} bytes; did file change during reading?`);

			// TODO: make sure file does not already exist? require additional flag to update?
			yield runQuery(
				client,
				`INSERT INTO "${KEYSPACE_PREFIX + stashInfo.name}".fs
				(pathname, parent, type, content, size, blake2b224, mtime, executable)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?);`,
				[dbPath, parentPath, type, content, size, blake2b224, mtime, executable]
			);
		}
	}));
}

/**
 * Put files or directories into the Cassandra database.
 */
function putFiles(pathnames) {
	return doWithClient(Promise.coroutine(function*(client) {
		for(const p of pathnames) {
			yield putFile(client, p);
		}
	}));
}

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

/**
 * Get a readable stream with the file contents, whether the file is in the db
 * or in a chunk store.
 */
const streamFile = Promise.coroutine(function*(client, stashInfo, dbPath) {
	// TODO: instead of checking just this one stash, check all stashes
	const storeName = stashInfo.chunkStore;
	if(!storeName) {
		throw new Error("stash info doesn't specify chunkStore key");
	}

	let result;
	try {
		result = yield runQuery(
			client,
			`SELECT pathname, size, type, key, "chunks_in_${storeName}", blake2b224, content, mtime, executable
			FROM "${KEYSPACE_PREFIX + stashInfo.name}".fs
			WHERE pathname = ?;`,
			[dbPath]
		);
	} catch(err) {
		if(!(/^ResponseError: Undefined name .* in selection clause/.test(String(err)))) {
			throw err;
		}
		// chunks_in_${storeName} doesn't exist, try the query without it
		result = yield runQuery(
			client,
			`SELECT pathname, size, type, key, blake2b224, content, mtime, executable
			FROM "${KEYSPACE_PREFIX + stashInfo.name}".fs
			WHERE pathname = ?;`,
			[dbPath]
		);
	}
	A.lte(result.rows.length, 1);
	if(result.rows.length === 0) {
		throw new NoSuchPathError(`Path ${inspect(dbPath)} not in stash ${inspect(stashInfo.name)}`);
	}
	const row = result.rows[0];
	if(row.type !== 'f') {
		throw new NotAFileError(`Path ${inspect(dbPath)} in stash ${inspect(stashInfo.name)} is not a file`);
	}

	const chunkStore = (yield getChunkStores()).stores[storeName];
	const chunks = row['chunks_in_' + storeName];
	let hasher;
	if(chunks) {
		A.eq(row.content, null);
		A.eq(row.key.length, 128/8);
		let cipherStream;
		if(chunkStore.type === "localfs") {
			if(!localfs) {
				localfs = require('./chunker/localfs');
			}
			const chunksDir = chunkStore.directory;
			cipherStream = localfs.readChunks(chunksDir, chunks);
		} else {
			if(!gdrive) {
				gdrive = require('./chunker/gdrive');
			}
			const gdriver = new gdrive.GDriver(chunkStore.clientId, chunkStore.clientSecret);
			yield gdriver.loadCredentials();
			cipherStream = gdrive.readChunks(gdriver, chunks);
		}
		const clearStream = crypto.createCipheriv('aes-128-ctr', row.key, iv0);
		cipherStream.pipe(clearStream);
		cipherStream.on('error', function(err) {
			clearStream.emit('error', err);
		});
		if(!padded_stream) {
			padded_stream = require('./padded_stream');
		}
		const unpadder = new padded_stream.Unpadder(Number(row.size));
		clearStream.pipe(unpadder);
		hasher = utils.streamHasher(unpadder, 'blake2b');
	} else {
		const streamWrapper = streamifier.createReadStream(row.content);
		hasher = utils.streamHasher(streamWrapper, 'blake2b');
	}
	hasher.stream.once('end', function() {
		if(hasher.length !== Number(row.size)) {
			hasher.stream.emit('error', new Error(
				`For dbPath=${dbPath}, expected length of content to be\n` +
				`${utils.numberWithCommas(row.size)} but was\n` +
				`${utils.numberWithCommas(hasher.length)}`
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
	return doWithPath(stashName, p, Promise.coroutine(function*(stashInfo, dbPath, parentPath) {
		const _ = yield streamFile(client, stashInfo, dbPath);
		const row = _[0];
		const readStream = _[1];

		let outputFilename;
		// If stashName was given, write file to current directory
		if(stashName) {
			outputFilename = row.pathname;
		} else {
			outputFilename = stashInfo.path + '/' + row.pathname;
		}

		yield utils.mkdirpAsync(path.dirname(outputFilename));

		const writeStream = fs.createWriteStream(outputFilename);
		readStream.pipe(writeStream);
		yield new Promise(function(resolve, reject) {
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
		yield utils.utimesAsync(outputFilename, row.mtime, row.mtime);
		if(row.executable) {
			// TODO: setting for 0o700 instead?
			yield utils.chmodAsync(outputFilename, 0o770);
		}
	}));
}

function getFiles(stashName, pathnames) {
	return doWithClient(Promise.coroutine(function*(client) {
		for(const p of pathnames) {
			yield getFile(client, stashName, p);
		}
	}));
}

function catFile(client, stashName, p) {
	return doWithPath(stashName, p, Promise.coroutine(function*(stashInfo, dbPath, parentPath) {
		const _ = yield streamFile(client, stashInfo, dbPath);
		//const row = _[0];
		const readStream = _[1];
		readStream.pipe(process.stdout);
	}));
}

function catFiles(stashName, pathnames) {
	return doWithClient(Promise.coroutine(function*(client) {
		for(const p of pathnames) {
			yield catFile(client, stashName, p);
		}
	}));
}

function dropFile(client, stashName, p) {
	return doWithPath(stashName, p, function(stashInfo, dbPath, parentPath) {
		//console.log({stashInfo, dbPath, parentPath});
		return runQuery(
			client,
			`DELETE FROM "${KEYSPACE_PREFIX + stashInfo.name}".fs
			WHERE pathname = ?;`,
			[dbPath]
		);
	});
}

/**
 * Remove files from the Cassandra database and their corresponding chunks.
 */
function dropFiles(stashName, pathnames) {
	return doWithClient(Promise.coroutine(function*(client) {
		for(const p of pathnames) {
			yield dropFile(client, stashName, p);
		}
	}));
}

/**
 * List all terastash keyspaces in Cassandra
 */
function listTerastashKeyspaces() {
	return doWithClient(function(client) {
		// TODO: also display durable_writes, strategy_class, strategy_options  info in table
		return runQuery(
			client,
			`SELECT keyspace_name FROM System.schema_keyspaces;`
		).then(function(result) {
			for(const row of result.rows) {
				const name = row.keyspace_name;
				if(name.startsWith(KEYSPACE_PREFIX)) {
					console.log(name.replace(KEYSPACE_PREFIX, ""));
				}
			}
		});
	});
}

const listChunkStores = Promise.coroutine(function*() {
	const config = yield getChunkStores();
	for(const storeName of Object.keys(config.stores)) {
		console.log(storeName);
	}
});

const defineChunkStore = Promise.coroutine(function*(storeName, opts) {
	T(storeName, T.string, opts, T.object);
	const config = yield getChunkStores();
	if(utils.hasKey(config.stores, storeName)) {
		throw new Error(`${storeName} is already defined in chunk-stores.json`);
	}
	if(typeof opts.chunkSize !== "number") {
		throw new Error(`.chunkSize is missing or not a number on ${inspect(opts)}`);
	}
	const storeDef = {type: opts.type, chunkSize: opts.chunkSize};
	if(opts.type === "localfs") {
		if(typeof opts.directory !== "string") {
			throw new Error(`Chunk store type localfs requires a -d/--directory ` +
				`parameter with a string; got ${opts.directory}`
			);
		}
		storeDef.directory = opts.directory;
	} else if(opts.type === "gdrive") {
		if(typeof opts.clientId !== "string") {
			throw new Error(`Chunk store type gdrive requires a --client-id ` +
				`parameter with a string; got ${opts.clientId}`
			);
		}
		storeDef.clientId = opts.clientId;
		if(typeof opts.clientSecret !== "string") {
			throw new Error(`Chunk store type gdrive requires a --client-secret ` +
				`parameter with a string; got ${opts.clientSecret}`
			);
		}
		storeDef.clientSecret = opts.clientSecret;
	} else {
		throw new Error(`Type must be "localfs" or "gdrive" but was ${opts.type}`);
	}
	config.stores[storeName] = storeDef;
	yield utils.writeObjectToConfigFile("chunk-stores.json", config);
});

const configChunkStore = Promise.coroutine(function*(storeName, opts) {
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
	return new Promise(function(resolve) {
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

const authorizeGDrive = Promise.coroutine(function*(name) {
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

const destroyStash = Promise.coroutine(function*(stashName) {
	assertName(stashName);
	yield doWithClient(function(client) {
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
const initStash = Promise.coroutine(function*(stashPath, stashName, options) {
	T(
		stashPath, T.string,
		stashName, T.string,
		options, T.shape({
			chunkStore: T.string,
			chunkThreshold: T.number
		})
	);
	assertName(stashName);

	if(yield getStashInfoByPath(stashPath)) {
		throw new Error(`${stashPath} is already configured as a stash`);
	}

	return doWithClient(Promise.coroutine(function*(client) {
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
			pathname text PRIMARY KEY,
			type ascii,
			parent text,
			size bigint,
			content blob,
			blake2b224 blob,
			key blob,
			mtime timestamp,
			crtime timestamp,
			executable boolean
		);`);

		// Note: chunks_in_* columns are added by defineChunkStore.
		// We use column-per-chunk-store instead of having a map of
		// <chunkStore, chunkInfo> because non-frozen, nested collections
		// aren't implemented: https://issues.apache.org/jira/browse/CASSANDRA-7826

		yield runQuery(client, `CREATE INDEX IF NOT EXISTS fs_parent
			ON "${KEYSPACE_PREFIX + stashName}".fs (parent);`);

		const config = yield getStashes();
		config.stashes[stashName] = {
			path: path.resolve(stashPath),
			chunkStore: options.chunkStore,
			chunkThreshold: options.chunkThreshold
		};
		yield utils.writeObjectToConfigFile("stashes.json", config);
	}));
});

function dumpDb(stashName) {
	T(stashName, T.maybe(T.string));
	return doWithClient(function(client) {
		return doWithPath(stashName, ".", Promise.coroutine(function*(stashInfo, dbPath, parentPath) {
			T(stashInfo.name, T.string);
			if(!transit) {
				transit = require('transit-js');
			}
			if(!cassandra) {
				loadCassandra();
			}
			if(!objectAssign) {
				objectAssign = require('object-assign');
			}
			const writer = transit.writer("json-verbose", {handlers: transit.map([
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
			const result = yield runQuery(client, `SELECT * FROM "${KEYSPACE_PREFIX + stashInfo.name}".fs;`);
			for(const row of result.rows) {
				//console.log(row);
				console.log(writer.write(row));
			}
		}));
	});
}

module.exports = {
	initStash, destroyStash, getStashes, getChunkStores, authorizeGDrive,
	listTerastashKeyspaces, listChunkStores, defineChunkStore, configChunkStore,
	putFile, putFiles, getFile, getFiles, catFile, catFiles, dropFile, dropFiles,
	lsPath, KEYSPACE_PREFIX, dumpDb, NoSuchPathError, NotAFileError
};
