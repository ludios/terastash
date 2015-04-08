"use strict";

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
 * This is our dumb hack to work around commander's lack of proper wrapping.
 */
function d(s) {
	return s.replace(/\t+/g, "\t") + "\n";
}

program
	.version('0.0.1');

program
	.command('init <name>')
	.description(d(`
		Initializes a stash in this directory and creates corresponding
		Cassandra keyspace with name ${terastash.CASSANDRA_KEYSPACE_PREFIX}<name>. Name cannot be changed later.`))
	.action(function(name) {
		assert(typeof name == "string", name);
		terastash.initStash(process.cwd(), name);
	});

program
	.command('destroy <name>')
	.description(d(`
		Destroys Cassandra keyspace ${terastash.CASSANDRA_KEYSPACE_PREFIX}<name>`))
	.action(function(name) {
		assert(typeof name == "string", name);
		terastash.destroyKeyspace(name);
	});

program
	.command('add <file...>')
	.description(d(`
		Adds file(s) to database`))
	.action(function(files) {
		// TODO: support -n
		terastash.addFiles(files);
	});

program
	.command('nuke <file...>')
	.description(d(`
		Removes file(s) from database and their corresponding chunks, if any.
		Does not emit error or warning if specified files are not in the database.
		Does not remove corresponding local checkout of the file.`))
	.action(function(files) {
		// TODO: support -n
		terastash.nukeFiles(files);
	});

program
	.command('help')
	.description(d(`
		Output usage information`))
	.action(function() {
		program.help();
	});

program
	.command('ls [path...]')
	.description(d(`
		List directory in the database`))
	.option('-n, --name <name>', 'Ignore .terastash.json and use this stash name')
	.action(function(paths, options) {
		//console.log({cmd, options}); process.exit();
		const name = stringOrNull(options.name);
		if(name != null && !paths.length) {
			console.error("When using -n/--name, a database path is required");
			process.exit(1);
		}
		terastash.lsPath(name, paths[0] || '.');
	});

program
	.command('list-keyspaces')
	.description(d(`
		List all terastash keyspaces in Cassandra`))
	.action(function(cmd, options) {
		terastash.listKeyspaces();
	});

program.parse(process.argv);

console.log(`Invalid arguments: ${JSON.stringify(program.args)}`);
program.help();
