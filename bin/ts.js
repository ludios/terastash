"use strict";

require('better-buffer-inspect');

const terastash = require('..');
const assert = require('assert');
const program = require('commander');

// Ugly hack to avoid getting Function
function stringOrNull(o) {
	return typeof o == "string" ? o : null;
}

/**
 * Replace repeating tabs and add trailing newline.
 *
 * This is our dumb hack to work around commander's lack of wrapping.
 */
function d(s) {
	return s.replace(/\t+/g, "\t") + "\n";
}

/**
 * Commander doesn't detect invalid commands for us, so
 * track if one of our actions was called   We print an error
 * later if one wasn't.
 *
 * https://github.com/tj/commander.js/issues/338
 */
let ranCommand = false;
function a(f) {
	return function() {
		ranCommand = true;
		f.apply(this, arguments);
	};
}

program
	.version('0.0.1');

program
	.command('init <name>')
	.description(d(`
		Initializes a stash in this directory and creates corresponding
		Cassandra keyspace with name ${terastash.CASSANDRA_KEYSPACE_PREFIX}<name>. Name cannot be changed later.`))
	.action(a(function(name) {
		assert(typeof name == "string", name);
		terastash.initStash(process.cwd(), name);
	}));

program
	.command('destroy <name>')
	.description(d(`
		Destroys Cassandra keyspace ${terastash.CASSANDRA_KEYSPACE_PREFIX}<name>`))
	.action(a(function(name) {
		assert(typeof name == "string", name);
		terastash.destroyKeyspace(name);
	}));

/* It's 'add' instead of 'put' for left-hand-only typing */
program
	.command('add <path...>')
	.description(d(`
		Put a file or directory (recursively) into the database`))
	.action(a(function(files) {
		terastash.putFiles(files);
	}));

program
	.command('get <path...>')
	.option('-n, --name <name>', 'Ignore .terastash.json and use this stash name')
	.description(d(`
		Get a file or directory (recursively) from the database`))
	.action(a(function(files, options) {
		const name = stringOrNull(options.name);
		terastash.getFiles(name, files);
	}));

program
	.command('cat <file...>')
	.option('-n, --name <name>', 'Ignore .terastash.json and use this stash name')
	.description(d(`
		Dump the contents of a file in the database to stdout`))
	.action(a(function(files, options) {
		const name = stringOrNull(options.name);
		terastash.catFiles(name, files);
	}));

program
	.command('drop <file...>')
	.option('-n, --name <name>', 'Ignore .terastash.json and use this stash name')
	.description(d(`
		Removes file(s) from the database and their corresponding chunks, if any.
		Does not emit error or warning if specified files are not in the database.
		Does not remove the corresponding file in the stash directory, if it is there.`))
	.action(a(function(files, options) {
		const name = stringOrNull(options.name);
		terastash.dropFiles(name, files);
	}));

program
	.command('ls [path...]')
	.description(d(`
		List directory in the database`))
	.option('-n, --name <name>', 'Ignore .terastash.json and use this stash name')
	.option('-j, --just-names', 'Print just the filenames without any decoration')
	.action(a(function(paths, options) {
		//console.log({cmd, options}); process.exit();
		const name = stringOrNull(options.name);
		if(name != null && !paths.length) {
			console.error("When using -n/--name, a database path is required");
			process.exit(1);
		}
		// When not using -n, and no path given, use '.'
		if(name == null && paths[0] == null) {
			paths[0] = '.';
		}
		terastash.lsPath(name, options.justNames, paths[0]);
	}));

program
	.command('list-stashes')
	.description(d(`
		List all terastash keyspaces in Cassandra`))
	.action(a(function(cmd, options) {
		terastash.listStashes();
	}));

program
	.command('help')
	.description(d(`
		Output usage information`))
	.action(a(function() {
		program.help();
	}));

program.parse(process.argv);

if(!ranCommand) {
	console.log(`Unknown command: ${program.args[0]}; see ts help`)
	process.exit(1);
}
