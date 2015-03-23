import fs from 'fs';
import assert from 'assert';
import path from 'path';
import cassandra from 'cassandra-driver';
import { sync as findParentDir } from 'find-parent-dir';

export const CASSANDRA_KEYSPACE_PREFIX = "ts_";

function getNewClient() {
	return new cassandra.Client({contactPoints: ['localhost']});
}

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
	// TODO: Don't use dotfile-in-directory approach: we have the same
	// vulnerability as git did.  Instead, read from a global configuration
	// file listing all stashes.  Or read from Cassandra DB.
	return findParentDir(path.dirname(path.resolve(pathname)), ".terastash.json");
}

function getParentPath(path) {
	const parts = path.split('/');
	parts.pop();
	return parts.join('/');
}

export function lsPath(stashName, pathname) {
	const client = getNewClient();
	client.execute(`SELECT * from "${CASSANDRA_KEYSPACE_PREFIX + stashName}".fs
		WHERE parent = ?`,
		[pathname],
		function(err, result) {
			client.shutdown();
			assert.ifError(err);
			console.log(result.rows);
		})
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
	const stashInfo = getStashInfo(stashBase);
	const dbPath = pathname.replace(stashBase, "").replace(/\\/g, "/");
	//console.log({stashBase, dbPath});

	const client = getNewClient();
	// TODO: validate stashInfo.name - it may contain injection
	client.execute(`INSERT INTO "${CASSANDRA_KEYSPACE_PREFIX + stashInfo.name}".fs
		(pathname, parent, content) VALUES (?, ?, ?);`,
		[dbPath, getParentPath(dbPath), content],
		function(err, result) {
			client.shutdown();
			assert.ifError(err);
		}
	);
}

/**
 * Add files into the Cassandra database.
 */
export function addFiles(pathnames) {
	for(let p of pathnames) {
		addFile(p);
	}
}

/**
 * List all terastash keyspaces in Cassandra
 */
export function listKeyspaces() {
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

export function destroyKeyspace(name) {
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
export function ol(s) {
	return s.replace(/[\n\t]+/g, " ");
}

/**
 * Initialize a new stash
 */
export function initStash(stashPath, name) {
	assertName(name);

	if(getStashInfo(stashPath)) {
		throw new Error(`${stashPath} already contains a .terastash.json`);
	}

	const client = getNewClient();
	client.execute(
	`CREATE KEYSPACE IF NOT EXISTS "${CASSANDRA_KEYSPACE_PREFIX + name}"
	WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`, [], function(err, result) {
		client.shutdown();
		assert.ifError(err);

		client.execute(`CREATE TABLE IF NOT EXISTS "${CASSANDRA_KEYSPACE_PREFIX + name}".fs (
			pathname text PRIMARY KEY,
			parent text,
			content blob,
			sha256sum blob
		);`, [], function(err, result) {
			client.shutdown();
			assert.ifError(err);

			fs.writeFileSync(
				`${stashPath}/.terastash.json`,
				JSON.stringify({
					name: name,
					_comment: ol(`You cannot change the name because it must match the
						Cassandra keyspace, and you cannot rename a Cassandra keyspace.`)
				}, null, 2));

			console.log("Created .terastash.json and Cassandra keyspace.");
		});
	});
}
