"use strict";

const fs = require('fs');
const assert = require('assert');
const path = require('path');
const cassandra = require('cassandra-driver');
const co = require('co');
const mkdirp = require('mkdirp');
const basedir = require('xdg').basedir;

const CASSANDRA_KEYSPACE_PREFIX = "ts_";

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
			_comment: ol(`You cannot change the name of a stash because it must match
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

function getParentPath(path) {
	const parts = path.split('/');
	parts.pop();
	return parts.join('/');
}

function canonicalizePathname(pathname) {
	pathname = pathname.replace(/\/+/g, "/");
	pathname = pathname.replace(/\/$/g, "");
	return pathname;
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
 * ISO-ish string without the seconds
 */
function shortISO(d) {
	return d.toISOString().substr(0, 16).replace("T", " ");
}

function pad(s, wantLength) {
	const len = s.length;
	if(len >= wantLength) {
		return s;
	}
	return " ".repeat(wantLength - len) + s;
}

// http://stackoverflow.com/questions/2901102/how-to-print-a-number-with-commas-as-thousands-separators-in-javascript
function numberWithCommas(s_or_n) {
	return ("" + s_or_n).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function lsPath(stashName, p) {
	doWithPath(stashName, p, function(client, stashInfo, dbPath, parentPath) {
		client.execute(`SELECT pathname, size, mtime, executable from "${CASSANDRA_KEYSPACE_PREFIX + stashInfo.name}".fs
			WHERE parent = ?`,
			[dbPath],
			{prepare: true},
			function(err, result) {
				client.shutdown();
				assert.ifError(err);
				for(let row of result.rows) {
					let nameWithExec = row.pathname;
					if(row.executable) {
						nameWithExec += '*';
					}
					console.log(
						pad(numberWithCommas(row.size.toString()), 18) + " " +
						shortISO(row.mtime) + " " +
						nameWithExec
					);
				}
			}
		);
	});
}

function doWithPath(stashName, p, f) {
	const client = getNewClient();
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

	const parentPath = getParentPath(dbPath);
	assert(!parentPath.startsWith('/'), parentPath);

	// TODO: validate stashInfo.name - it may contain injection
	f(client, stashInfo, dbPath, parentPath);
}

/* also called S_IXUSR */
const S_IEXEC = parseInt('0100', 8);

/**
 * Put a file or directory into the Cassandra database.
 */
function putFile(p) {
	doWithPath(null, p, function(client, stashInfo, dbPath, parentPath) {
		const content = fs.readFileSync(p);
		const stat = fs.statSync(p);
		const mtime = stat.mtime;
		const size = content.length;
		const executable = Boolean(stat.mode & S_IEXEC);

		// TODO: make sure it does not already exist? require additional flag to update?
		client.execute(`INSERT INTO "${CASSANDRA_KEYSPACE_PREFIX + stashInfo.name}".fs
			(pathname, parent, content, size, mtime, executable) VALUES (?, ?, ?, ?, ?, ?);`,
			[dbPath, parentPath, content, size, mtime, executable],
			{prepare: true},
			function(err, result) {
				client.shutdown();
				assert.ifError(err);
			}
		);
	});
}

/**
 * Put files or directories into the Cassandra database.
 */
function putFiles(pathnames) {
	for(let p of pathnames) {
		putFile(p);
	}
}

/**
 * Get a file or directory from the Cassandra database.
 */
function getFile(stashName, p) {
	doWithPath(stashName, p, function(client, stashInfo, dbPath, parentPath) {
		client.execute(`SELECT pathname, content FROM "${CASSANDRA_KEYSPACE_PREFIX + stashInfo.name}".fs
			WHERE pathname = ?;`,
			[dbPath],
			{prepare: true},
			function(err, result) {
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
				client.shutdown();
				assert.ifError(err);
			}
		);
	});
}

/**
 * Get files or directories from the Cassandra database.
 */
function getFiles(stashName, pathnames) {
	for(let p of pathnames) {
		getFile(stashName, p);
	}
}

function catFile(stashName, p) {
	doWithPath(stashName, p, function(client, stashInfo, dbPath, parentPath) {
		client.execute(`SELECT content FROM "${CASSANDRA_KEYSPACE_PREFIX + stashInfo.name}".fs
			WHERE pathname = ?;`,
			[dbPath],
			{prepare: true},
			function(err, result) {
				for(let row of result.rows) {
					process.stdout.write(row.content);
				}
				client.shutdown();
				assert.ifError(err);
			}
		);
	});
}

function catFiles(stashName, pathnames) {
	for(let p of pathnames) {
		catFile(stashName, p);
	}
}

function dropFile(stashName, p) {
	doWithPath(stashName, p, function(client, stashInfo, dbPath, parentPath) {
		//console.log({stashInfo, dbPath, parentPath});
		client.execute(`DELETE FROM "${CASSANDRA_KEYSPACE_PREFIX + stashInfo.name}".fs
			WHERE pathname = ?;`,
			[dbPath],
			{prepare: true},
			function(err, result) {
				client.shutdown();
				assert.ifError(err);
			}
		);
	});
}

/**
 * Remove files from the Cassandra database and their corresponding chunks.
 */
function dropFiles(stashName, pathnames) {
	for(let p of pathnames) {
		dropFile(stashName, p);
	}
}

/**
 * List all terastash keyspaces in Cassandra
 */
function listStashes() {
	const client = getNewClient();
	// TODO: also display durable_writes, strategy_class, strategy_options  info in table
	client.execute(`SELECT keyspace_name FROM System.schema_keyspaces;`, [], function(err, result) {
		client.shutdown();
		assert.ifError(err);
		for(let row of result.rows) {
			const name = row.keyspace_name;
			if(name.startsWith(CASSANDRA_KEYSPACE_PREFIX)) {
				console.log(name.replace(CASSANDRA_KEYSPACE_PREFIX, ""));
			}
		}
	});
}

function assertName(name) {
	assert(name, "Name must not be empty");
	assert(typeof name == 'string', `Name must be string, got ${typeof name}`);
}

function destroyKeyspace(name) {
	assertName(name);
	const client = getNewClient();
	client.execute(`DROP KEYSPACE "${CASSANDRA_KEYSPACE_PREFIX + name}";`, [], function(err, result) {
		client.shutdown();
		assert.ifError(err);
		console.log(`Destroyed keyspace ${CASSANDRA_KEYSPACE_PREFIX + name}.`);
	});
}

// TODO: function to destroy all keyspaces that no longer have a matching .terastash.json file
// TODO: need to store path to terastash base in a cassandra table

/**
 * Convert string with newlines and tabs to one without.
 */
function ol(s) {
	return s.replace(/[\n\t]+/g, " ");
}

function executeWithPromise(client, statement, args) {
	return new Promise(function(resolve, reject) {
		client.execute(statement, args, function(err, result) {
			if(err) {
				reject(err);
			} else {
				resolve(result);
			}
		});
	});
}

/**
 * Initialize a new stash
 */
function initStash(stashPath, name) {
	assertName(name);

	if(findStashInfoByPath(stashPath)) {
		throw new Error(`${stashPath} is already configured as a stash`);
	}

	const client = getNewClient();

	co(function*(){
		yield executeWithPromise(client, `CREATE KEYSPACE IF NOT EXISTS "${CASSANDRA_KEYSPACE_PREFIX + name}"
			WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`, []);

		yield executeWithPromise(client, `CREATE TABLE IF NOT EXISTS "${CASSANDRA_KEYSPACE_PREFIX + name}".fs (
			pathname text PRIMARY KEY,
			parent text,
			size bigint,
			content blob,
			sha256sum blob,
			mtime timestamp,
			crtime timestamp,
			executable boolean
		);`, []);

		yield executeWithPromise(client, `CREATE INDEX IF NOT EXISTS fs_parent
			ON "${CASSANDRA_KEYSPACE_PREFIX + name}".fs (parent);`, []);

		const config = getTerastashConfig();
		config['stashes'].push({name, path: path.resolve(stashPath)});
		writeTerastashConfig(config);

		console.log("Created Cassandra keyspace and updated terastash.json.");
		client.shutdown();
	}).catch(function(err) {
		console.error(err);
		client.shutdown();
	});
}

module.exports = {
	initStash, ol, destroyKeyspace, listStashes, putFile, putFiles, getFile, getFiles,
	catFile, catFiles, dropFile, dropFiles, lsPath, canonicalizePathname, getParentPath,
	CASSANDRA_KEYSPACE_PREFIX, pad}
