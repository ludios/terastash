import fs from 'fs';
import assert from 'assert';
import path from 'path';
import cassandra from 'cassandra-driver';
import co from 'co';
import { basedir } from 'xdg';

export const CASSANDRA_KEYSPACE_PREFIX = "ts_";

function getNewClient() {
	return new cassandra.Client({contactPoints: ['localhost']});
}

function getTerastashConfig() {
	const configPath = basedir.configPath("terastash.json");
	try {
		return JSON.parse(fs.readFileSync(configPath));
	} catch(e) {
		if(e.code != 'ENOENT') {
			throw e;
		}
		const config = {stashes: []};
		fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
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
		console.log(resolvedPathname, stash.path);
		if(resolvedPathname.startsWith(stash.path)) {
			return stash;
		}
	}
	return null;
}

export function getParentPath(path) {
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
		}
	);
}

/**
 * Add a file into the Cassandra database.
 */
export function addFile(pathname) {
	const resolvedPathname = path.resolve(pathname);
	const content = fs.readFileSync(pathname);
	const stashInfo = findStashInfo(resolvedPathname);
	if(!stashInfo) {
		throw new Error(`File ${pathname} is not inside a stash; edit terastash.json and add a stash`);
	}
	const dbPath = resolvedPathname.replace(stashInfo.path, "").replace(/\\/g, "/");

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
export function initStash(stashPath, name) {
	assertName(name);

	if(getStashInfo(stashPath)) {
		throw new Error(`${stashPath} already contains a .terastash.json`);
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

		fs.writeFileSync(
			`${stashPath}/.terastash.json`,
			JSON.stringify({
				name: name,
				_comment: ol(`You cannot change the name because it must match the
					Cassandra keyspace, and you cannot rename a Cassandra keyspace.`)
			}, null, 2)
		);

		console.log("Created .terastash.json and Cassandra keyspace.");
		client.shutdown();
	}).catch(function(err) {
		console.error(err);
		client.shutdown();
	});
}
