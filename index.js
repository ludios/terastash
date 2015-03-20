import fs from 'fs';
import assert from 'assert';
import path from 'path';
import cassandra from 'cassandra-driver';
import { sync as findParentDir } from 'find-parent-dir';

export const CASSANDRA_KEYSPACE_PREFIX = "ts_";

function getStashInfo(stashPath) {
	try {
		return JSON.parse(fs.readFileSync(`${stashPath}/.terastash.json`));
	} catch(e) {
		if(e.code != 'ENOENT') {
			throw e;
		}
	}
	return null;
}

/**
 * For a given pathname, return which directory is the terastash
 * base, or `null` if there is no terastash base.
 */
function findStashBase(pathname) {
	return findParentDir(path.dirname(path.resolve(pathname)), ".terastash.json");
}

/**
 * Add a file into the Cassandra database.
 */
export function addFile(pathname) {
	const content = fs.readFileSync(pathname);
	const stashBase = findStashBase(pathname);
	if(!stashBase) {
		throw new Error(`File ${pathname} is not inside a stash: could not find a .terastash.json in any parent directories.`);
	}
	const dbPath = pathname.replace(stashBase, "");
	//console.log({stashBase, dbPath});
}

/**
 * Add files into the Cassandra database.
 */
export function addFiles(pathnames) {
	for(let p of pathnames) {
		addFile(p);
	}
}

function getNewClient() {
	return new cassandra.Client({contactPoints: ['localhost']});
}

/**
 * List all terastash keyspaces in Cassandra
 */
export function listKeyspaces() {
	const client = getNewClient();
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

/**
 * Initialize a new stash
 */
export function initStash(stashPath, name) {
	assert(name, "Name must not be empty");
	assert(typeof name == 'string', `Name must be string, got ${typeof name}`);

	if(getStashInfo(stashPath)) {
		throw new Error(`${stashPath} already contains a .terastash.json`);
	}

	const client = getNewClient();
	client.execute(`CREATE KEYSPACE IF NOT EXISTS "${CASSANDRA_KEYSPACE_PREFIX}${name}" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`, [], function(err, result) {
		client.shutdown();
		assert.ifError(err);
		console.log(result);
	});

	fs.writeFileSync(
		`${stashPath}/.terastash.json`,
		JSON.stringify({name: name}, null, 2));
}
