import fs from 'fs';
import assert from 'assert';

/**
 * Add a file into the Cassandra database.
 */
export function addFile(pathname) {
	let content = fs.readFileSync(pathname);
}

/**
 * Add files into the Cassandra database.
 */
export function addFiles(pathnames) {
	for(let p of pathnames) {
		addFile(p);
	}
}

export function getStashInfo(stashPath) {
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
 * Initialize a new stash
 */
export function initStash(stashPath, name) {
	assert(name, "Name must not be empty");
	assert(typeof name == 'string', `Name must be string, got ${typeof name}`);

	if(getStashInfo(stashPath)) {
		throw new Error(`${stashPath} already contains a .terastash.json`);
	}
	fs.writeFileSync(
		`${stashPath}/.terastash.json`,
		JSON.stringify({name: name}, null, 2));
}
