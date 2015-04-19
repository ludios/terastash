"use strict";

const fs = require('fs');
const assert = require('assert');
const path = require('path');
const cassandra = require('cassandra-driver');
const co = require('co');
const mkdirp = require('mkdirp');
const basedir = require('xdg').basedir;
const chalk = require('chalk');

const utils = require('./utils');

const KEYSPACE_PREFIX = "ts_";

function getNewClient() {
	return new cassandra.Client({contactPoints: ['localhost']});
}

function writeTerastashConfig(config) {
	const configPath = basedir.configPath("terastash.json");
	mkdirp(path.dirname(configPath));
	fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
}

function getTerastashConfig() {
	const configPath = basedir.configPath("terastash.json");
	try {
		return JSON.parse(fs.readFileSync(configPath));
	} catch(e) {
		if(e.code != 'ENOENT') {
			throw e;
		}
		// If there is no config file, write one.
		const config = {
			stashes: [],
			_comment: utils.ol(`You cannot change the name of a stash because it must match
				the Cassandra keyspace, and you cannot rename a Cassandra keyspace.`)};
		writeTerastashConfig(config);
		return config;
	}
}

/**
 * For a given pathname, return a stash that contains the file,
 * or `null` if there is no terastash base.
 */
function findStashInfoByPath(pathname) {
	const config = getTerastashConfig();
	if(!config.stashes || !Array.isArray(config.stashes)) {
		throw new Error(`terastash config has no "stashes" or not an Array`)
	}

	const resolvedPathname = path.resolve(pathname);
	for(let stash of config.stashes) {
		//console.log(resolvedPathname, stash.path);
		if(resolvedPathname.startsWith(stash.path)) {
			return stash;
		}
	}
	return null;
}

/**
 * Return a stash for a given stash name
 */
function findStashInfoByName(stashName) {
	const config = getTerastashConfig();
	if(!config.stashes || !Array.isArray(config.stashes)) {
		throw new Error(`terastash config has no "stashes" or not an Array`)
	}

	for(let stash of config.stashes) {
		if(stash.name == stashName) {
			return stash;
		}
	}
	return null;
}

/**
 * For any given relative user path, which may include ../, return
 * the corresponding path that should be used in the Cassandra
 * database.
 */
function userPathToDatabasePath(base, p) {
	const resolved = path.resolve(p);
	if(resolved == base) {
		return "";
	} else {
		const dbPath = resolved.replace(base + "/", "").replace(/\\/g, "/");
		assert(!dbPath.startsWith('/'), dbPath);
		return dbPath;
	}
}

/**
 * Run a Cassandra query and return a Promise that is fulfilled
 * with the query results.
 */
function runQuery(client, statement, args) {
	//console.log('runQuery(%s, %s, %s)', client, statement, args);
	assert(typeof client == 'object');
	assert(typeof statement == 'string');
	assert(Array.isArray(args));

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

function lsPath(stashName, justNames, p) {
	return doWithClient(function(client) {
		return doWithPath(client, stashName, p, function(client, stashInfo, dbPath, parentPath) {
			return runQuery(
				client,
				`SELECT pathname, type, size, mtime, executable
				from "${KEYSPACE_PREFIX + stashInfo.name}".fs
				WHERE parent = ?`,
				[dbPath]
			).then(function(result) {
				for(let row of result.rows) {
					const baseName = utils.getBaseName(row.pathname);
					if(justNames) {
						console.log(baseName);
					} else {
						let decoratedName = baseName;
						if(row.type == 'd') {
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
			})
		});
	});
}

function doWithPath(client, stashName, p, fn) {
	const resolvedPathname = path.resolve(p);
	let dbPath;
	let stashInfo;
	if(stashName) { // Explicit stash name provided
		stashInfo = findStashInfoByName(stashName);
		if(!stashInfo) {
			throw new Error(`No stash with name ${stashName}; consult terastash.json and ts help`);
		}
		dbPath = p;
	} else {
		stashInfo = findStashInfoByPath(resolvedPathname);
		if(!stashInfo) {
			throw new Error(`File ${p} is not in a stash directory; consult terastash.json and ts help`);
		}
		dbPath = userPathToDatabasePath(stashInfo.path, p);
	}

	const parentPath = utils.getParentPath(dbPath);
	assert(!parentPath.startsWith('/'), parentPath);

	// TODO: validate stashInfo.name - it may contain injection
	return fn(client, stashInfo, dbPath, parentPath);
}

/* also called S_IXUSR */
const S_IEXEC = parseInt('0100', 8);

function shouldStoreInChunks(p, stat) {
	return stat.size > 200*1024;
}

function doWithClient(f) {
	const client = getNewClient();
	const p = f(client);
	return p.catch(function(err) {
		console.error(err.stack);
	}).then(function() {
		client.shutdown();
	});
}

function makeDirs(client, stashInfo, p, dbPath) {
	return co(function*() {
		const type = 'd';
		const stat = fs.statSync(p);
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
}

/**
 * Put a file or directory into the Cassandra database.
 */
function putFile(client, p) {
	return doWithPath(client, null, p, co.wrap(function*(client, stashInfo, dbPath, parentPath) {
		const type = 'f';
		const stat = fs.statSync(p);
		const mtime = stat.mtime;
		const executable = Boolean(stat.mode & S_IEXEC);
		let content;
		let size;
		if(shouldStoreInChunks(p, stat)) {
			content = null;
			size = stat.size;
			/* TODO: later need to make sure that size is consistent with
			    what we've actually read from the file. */
		} else {
			content = fs.readFileSync(p);
			size = content.length;
		}

		if(parentPath) {
			yield makeDirs(client, stashInfo, path.dirname(p), parentPath);
		}

		// TODO: make sure it does not already exist? require additional flag to update?
		yield runQuery(
			client,
			`INSERT INTO "${KEYSPACE_PREFIX + stashInfo.name}".fs
			(pathname, parent, type, content, size, mtime, executable) VALUES (?, ?, ?, ?, ?, ?, ?);`,
			[dbPath, parentPath, type, content, size, mtime, executable]
		);
	}));
}

/**
 * Put files or directories into the Cassandra database.
 */
function putFiles(pathnames) {
	return doWithClient(co.wrap(function*(client) {
		for(let p of pathnames) {
			yield putFile(client, p);
		}
	}));
}

/**
 * Get a file or directory from the Cassandra database.
 */
function getFile(client, stashName, p) {
	return doWithPath(client, stashName, p, function(client, stashInfo, dbPath, parentPath) {
		return runQuery(
			client,
			`SELECT pathname, content
			FROM "${KEYSPACE_PREFIX + stashInfo.name}".fs
			WHERE pathname = ?;`,
			[dbPath]
		).then(function(result) {
			//console.log(result);
			for(let row of result.rows) {
				// TODO: create directories if needed
				// If stashName was given, write file to current directory
				if(stashName) {
					fs.writeFileSync(row.pathname, row.content);
				} else {
					fs.writeFileSync(stashInfo.path + '/' + row.pathname, row.content);
				}
			}
		});
	});
}

function getFiles(stashName, pathnames) {
	return doWithClient(co.wrap(function*(client) {
		for(let p of pathnames) {
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
			for(let row of result.rows) {
				process.stdout.write(row.content);
			}
		});
	});
}

function catFiles(stashName, pathnames) {
	return doWithClient(co.wrap(function*(client) {
		for(let p of pathnames) {
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
	return doWithClient(co.wrap(function*(client) {
		for(let p of pathnames) {
			yield dropFile(client, stashName, p);
		}
	}));
}

/**
 * List all terastash keyspaces in Cassandra
 */
function listStashes() {
	return doWithClient(function(client) {
		// TODO: also display durable_writes, strategy_class, strategy_options  info in table
		return runQuery(
			client,
			`SELECT keyspace_name FROM System.schema_keyspaces;`,
			[]
		).then(function(result) {
			for(let row of result.rows) {
				const name = row.keyspace_name;
				if(name.startsWith(KEYSPACE_PREFIX)) {
					console.log(name.replace(KEYSPACE_PREFIX, ""));
				}
			}
		});
	});
}

function assertName(name) {
	assert(name, "Name must not be empty");
	assert(typeof name == 'string', `Name must be string, got ${typeof name}`);
}

function destroyKeyspace(name) {
	assertName(name);
	return doWithClient(function(client) {
		return runQuery(
			client,
			`DROP KEYSPACE "${KEYSPACE_PREFIX + name}";`,
			[]
		).then(function() {
			console.log(`Destroyed keyspace ${KEYSPACE_PREFIX + name}.`);
		});
	});
}

// TODO: function to destroy all keyspaces that no longer have a matching .terastash.json file
// TODO: need to store path to terastash base in a cassandra table

/**
 * Initialize a new stash
 */
function initStash(stashPath, name) {
	assertName(name);

	if(findStashInfoByPath(stashPath)) {
		throw new Error(`${stashPath} is already configured as a stash`);
	}

	return doWithClient(co.wrap(function*(client) {
		yield runQuery(client, `CREATE KEYSPACE IF NOT EXISTS "${KEYSPACE_PREFIX + name}"
			WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`, []);

		yield runQuery(client, `CREATE TABLE IF NOT EXISTS "${KEYSPACE_PREFIX + name}".fs (
			pathname text PRIMARY KEY,
			type ascii,
			parent text,
			size bigint,
			content blob,
			chunks list<blob>,
			checksum blob,
			password blob,
			mtime timestamp,
			crtime timestamp,
			executable boolean
		);`, []);

		yield runQuery(client, `CREATE INDEX IF NOT EXISTS fs_parent
			ON "${KEYSPACE_PREFIX + name}".fs (parent);`, []);

		const config = getTerastashConfig();
		config['stashes'].push({name, path: path.resolve(stashPath)});
		writeTerastashConfig(config);

		console.log("Created Cassandra keyspace and updated terastash.json.");
	}));
}

module.exports = {
	initStash, destroyKeyspace, listStashes, putFile, putFiles, getFile, getFiles,
	catFile, catFiles, dropFile, dropFiles, lsPath, KEYSPACE_PREFIX}
