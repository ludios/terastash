"use strict";

const fs = require('fs');
const assert = require('assert');
const path = require('path');
const cassandra = require('cassandra-driver');
const co = require('co');
const basedir = require('xdg').basedir;

const CASSANDRA_KEYSPACE_PREFIX = "ts_";

function getNewClient() {
	return new cassandra.Client({contactPoints: ['localhost']});
}

function writeTerastashConfig(config) {
	const configPath = basedir.configPath("terastash.json");
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
function findStashInfo(pathname) {
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

function getParentPath(path) {
	const parts = path.split('/');
	parts.pop();
	return parts.join('/');
}

function canonicalizePathname(pathname) {
	if(!pathname.startsWith('/')) {
		pathname = '/' + pathname;
	}
	pathname = pathname.replace(/\/+/g, "/");
	pathname = pathname.replace(/\/$/g, "");
	return pathname;
}

function lsPath(stashName, pathname) {
	if(!stashName) {
		stashName = findStashInfo(pathname).name;
	}
	pathname = canonicalizePathname(pathname);
	const client = getNewClient();
	client.execute(`SELECT * from "${CASSANDRA_KEYSPACE_PREFIX + stashName}".fs
		WHERE parent = ?`,
		[pathname],
		function(err, result) {
			client.shutdown();
			assert.ifError(err);
			console.log(result.rows);
		}
	);
}

/**
 * Add a file into the Cassandra database.
 */
function addFile(pathname) {
	const resolvedPathname = path.resolve(pathname);
	const content = fs.readFileSync(pathname);
	const stashInfo = findStashInfo(resolvedPathname);
	if(!stashInfo) {
		throw new Error(`File ${pathname} is not inside a stash; edit terastash.json and add a stash`);
	}
	const dbPath = resolvedPathname.replace(stashInfo.path, "").replace(/\\/g, "/");
	assert(dbPath.startsWith('/'), dbPath);
	const parentPath = getParentPath(dbPath);
	assert(parentPath.startsWith('/'), parentPath);

	const client = getNewClient();
	// TODO: validate stashInfo.name - it may contain injection
	client.execute(`INSERT INTO "${CASSANDRA_KEYSPACE_PREFIX + stashInfo.name}".fs
		(pathname, parent, content) VALUES (?, ?, ?);`,
		[dbPath, parentPath, content],
		function(err, result) {
			client.shutdown();
			assert.ifError(err);
		}
	);
}

/**
 * Add files into the Cassandra database.
 */
function addFiles(pathnames) {
	for(let p of pathnames) {
		addFile(p);
	}
}

/**
 * List all terastash keyspaces in Cassandra
 */
function listKeyspaces() {
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

	if(findStashInfo(stashPath)) {
		throw new Error(`${stashPath} is already configured as a stash`);
	}

	const client = getNewClient();

	co(function*(){
		yield executeWithPromise(client, `CREATE KEYSPACE IF NOT EXISTS "${CASSANDRA_KEYSPACE_PREFIX + name}"
			WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`, []);

		yield executeWithPromise(client, `CREATE TABLE IF NOT EXISTS "${CASSANDRA_KEYSPACE_PREFIX + name}".fs (
			pathname text PRIMARY KEY,
			parent text,
			content blob,
			sha256sum blob
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

module.exports = {initStash, ol, destroyKeyspace, listKeyspaces, addFiles, addFile, lsPath, canonicalizePathname, getParentPath, CASSANDRA_KEYSPACE_PREFIX}
