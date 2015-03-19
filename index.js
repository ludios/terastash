import fs from 'fs';
import assert from 'assert';

/**
 * Add a file into the Cassandra database.
 */
function addFile(pathname) {
	let content = fs.readFileSync(pathname);
}

//console.log(process.argv);
if(0 && process.argv.length) {
	let command = process.argv[2];
	if(command == 'add') {
		let pathname = process.argv[3];
		assert(pathname, "Need a file to add");
		addFile(pathname);
	}
}
