import fs from 'fs';
import assert from 'assert';
import path from 'path';
import { sync as findParentDir } from 'find-parent-dir';

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
