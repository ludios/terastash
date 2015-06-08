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

const utils = require('./utils');
const localfs = require('./chunker/localfs');
let cassandra;
let blake2;
let gdrive;
let readline;

const KEYSPACE_PREFIX = "ts_";

function blake2b224Buffer(buf) {
	T(buf, Buffer);
	if(!blake2) {
		blake2 = utils.requireBlake2();
	}
	return blake2.createHash('blake2b').update(buf).digest().slice(0, 224/8);
}

let CassandraClientType = T.object;

function getNewClient() {
	if(!cassandra) {
		cassandra = require('cassandra-driver');
		CassandraClientType = cassandra.Client;
	}
	return new cassandra.Client({contactPoints: ['localhost']});
}

const getStashes = utils.makeConfigFileInitializer(
	"stashes.json", {
		stashes: [],
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
	if(!config.stashes || !Array.isArray(config.stashes)) {
		throw new Error(`terastash config has no "stashes" or not an Array`);
	}

	const resolvedPathname = path.resolve(pathname);
	for(const stash of config.stashes) {
		//console.log(resolvedPathname, stash.path);
		if(resolvedPathname.startsWith(stash.path)) {
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
	if(!config.stashes || !Array.isArray(config.stashes)) {
		throw new Error(`terastash config has no "stashes" or not an Array`);
	}

	for(const stash of config.stashes) {
		if(stash.name === stashName) {
			return stash;
		}
	}
	return null;
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

const doWithPath = Promise.coroutine(function*(client, stashName, p, fn) {
	T(client, CassandraClientType, stashName, T.maybe(T.string), p, T.string, fn, T.function);
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
	return fn(client, stashInfo, dbPath, parentPath);
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
		return doWithPath(client, stashName, p, function(client, stashInfo, dbPath, parentPath) {
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

function shouldStoreInChunks(p, stat) {
	return stat.size > 200*1024;
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

/**
 * Put a file or directory into the Cassandra database.
 */
function putFile(client, p) {
	return doWithPath(client, null, p, Promise.coroutine(function*(client, stashInfo, dbPath, parentPath) {
		const type = 'f';
		const stat = yield utils.statAsync(p);
		const mtime = stat.mtime;
		const executable = Boolean(stat.mode & 0o100); /* S_IXUSR */
		const storeName = stashInfo['chunk-store'];
		if(!storeName) {
			throw new Error("stash info doesn't specify chunk-store key");
		}

		if(parentPath) {
			yield makeDirs(client, stashInfo, path.dirname(p), parentPath);
		}

		if(shouldStoreInChunks(p, stat)) {
			// TODO: validate storeName
			// TODO: do this query only if we fail to add a file
			yield tryCreateColumnOnStashTable(
				client, stashInfo.name, `chunks_in_${storeName}`, 'list<frozen<chunk>>');
			const blake2b224 = undefined;
			const key = crypto.randomBytes(128/8);
			const config = yield getChunkStores();
			const chunksDir = config.stores[storeName].directory;
			// TODO: give writeChunks a stream instead so that we can get our
			// own blake2b224
			const chunkInfo = yield localfs.writeChunks(chunksDir, key, p);
			T(chunkInfo, Array);
			const size = stat.size;
			/* TODO: later need to make sure that size is consistent with
			    what we've actually read from the file. */

			// TODO: make sure file does not already exist? require additional flag to update?
			yield runQuery(
				client,
				`INSERT INTO "${KEYSPACE_PREFIX + stashInfo.name}".fs
				(pathname, parent, type, key, chunks_in_${storeName}, size, blake2b224, mtime, executable)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);`,
				[dbPath, parentPath, type, key, chunkInfo, size, blake2b224, mtime, executable]
			);
		} else {
			const content = yield utils.readFileAsync(p);
			const blake2b224 = blake2b224Buffer(content);
			const size = content.length;

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

/**
 * Get a file or directory from the Cassandra database.
 */
function getFile(client, stashName, p) {
	return doWithPath(client, stashName, p, Promise.coroutine(function*(client, stashInfo, dbPath, parentPath) {
		// TODO: instead of checking just this one stash, check all stashes
		const storeName = stashInfo['chunk-store'];
		if(!storeName) {
			throw new Error("stash info doesn't specify chunk-store key");
		}

		const result = yield runQuery(
			client,
			`SELECT pathname, size, key, chunks_in_${storeName}, blake2b224, content
			FROM "${KEYSPACE_PREFIX + stashInfo.name}".fs
			WHERE pathname = ?;`,
			[dbPath]
		);

		const config = yield getChunkStores();
		const chunksDir = config.stores[storeName].directory;

		//console.log(result);
		for(const row of result.rows) {
			let outputFilename;
			// If stashName was given, write file to current directory
			if(stashName) {
				outputFilename = row.pathname;
			} else {
				outputFilename = stashInfo.path + '/' + row.pathname;
			}
			yield utils.mkdirpAsync(path.dirname(outputFilename));

			const chunks = row['chunks_in_' + storeName];
			if(chunks) {
				A.eq(row.content, null);
				A.eq(row.blake2b224, null);
				const readStream = localfs.readChunks(chunksDir, row.key, chunks);
				const writeStream = fs.createWriteStream(outputFilename);
				readStream.pipe(writeStream);
				// TODO: check file length
				const p = new Promise(function(resolve, reject) {
					writeStream.once('finish', function() {
						resolve();
					});
					writeStream.once('error', function(err) {
						reject(err);
					});
					readStream.once('error', function(err) {
						reject(err);
					});
				});
				return p;
			} else {
				let blake2b224 = blake2b224Buffer(row.content);
				if(Number(row.size) !== row.content.length) {
					throw new Error(`Size of ${row.pathname} should be ${row.size} but was ${row.content.length}`);
				}
				if(!row.blake2b224.equals(blake2b224)) {
					throw new Error(
						`Database says BLAKE2b-224 of ${row.pathname} is\n` +
						`${row.blake2b224.toString('hex')} but content was \n` +
						`${blake2b224.toString('hex')}`);
				}
				return utils.writeFileAsync(outputFilename, row.content);
			}
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
	return doWithPath(client, stashName, p, function(client, stashInfo, dbPath, parentPath) {
		return runQuery(
			client,
			`SELECT content FROM "${KEYSPACE_PREFIX + stashInfo.name}".fs
			WHERE pathname = ?;`,
			[dbPath]
		).then(function(result) {
			for(const row of result.rows) {
				process.stdout.write(row.content);
			}
		});
	});
}

function catFiles(stashName, pathnames) {
	return doWithClient(Promise.coroutine(function*(client) {
		for(const p of pathnames) {
			yield catFile(client, stashName, p);
		}
	}));
}

function dropFile(client, stashName, p) {
	return doWithPath(client, stashName, p, function(client, stashInfo, dbPath, parentPath) {
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
	const storeDef = {type: opts.type};
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

function destroyKeyspace(stashName) {
	assertName(stashName);
	return doWithClient(function(client) {
		return runQuery(
			client,
			`DROP KEYSPACE "${KEYSPACE_PREFIX + stashName}";`
		).then(function() {
			console.log(`Destroyed keyspace ${KEYSPACE_PREFIX + stashName}.`);
		});
	});
}

/**
 * Initialize a new stash
 */
const initStash = Promise.coroutine(function*(stashPath, stashName, storeName) {
	T(stashPath, T.string, stashName, T.string, storeName, T.string);
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
		config.stashes.push({
			"name": stashName,
			"path": path.resolve(stashPath),
			"chunk-store": storeName
		});
		yield utils.writeObjectToConfigFile("stashes.json", config);
	}));
});

module.exports = {
	initStash, destroyKeyspace, getStashes, getChunkStores, authorizeGDrive, listTerastashKeyspaces,
	listChunkStores, defineChunkStore, putFile, putFiles, getFile, getFiles, catFile, catFiles,
	dropFile, dropFiles, lsPath, KEYSPACE_PREFIX
};
