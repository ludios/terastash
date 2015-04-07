"use strict";

const terastash = require('.');
const ol = terastash.ol;
const assert = require('assert');
const program = require('commander');

// Ugly hack to avoid getting Function
function stringOrNull(o) {
	return typeof o == "string" ? o : null;
}

program
	.version('0.0.1');

program
	.command('init <name>')
	.description(ol(`initializes a stash in this directory and creates corresponding Cassandra
		keyspace with name ${terastash.CASSANDRA_KEYSPACE_PREFIX}<name>.
		Name cannot be changed later.`))
	.action(function(name) {
		assert(typeof name == "string", name);
		terastash.initStash(process.cwd(), name);
	});

program
	.command('destroy <name>')
	.description(`Destroys Cassandra keyspace ${terastash.CASSANDRA_KEYSPACE_PREFIX}<name>`)
	.action(function(name) {
		assert(typeof name == "string", name);
		terastash.destroyKeyspace(name);
	});

program
	.command('add <file...>')
	.description('adds file(s) to database')
	.action(function(files) {
		terastash.addFiles(files);
	});

program
	.command('rm <file...>')
	.description('removes file(s) from database')
	.action(function(files) {
		terastash.removeFiles(files);
	});

program
	.command('ls [path...]')
	.description('list directory in the database')
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
	.description('list all terastash keyspaces in Cassandra')
	.action(function(cmd, options) {
		terastash.listKeyspaces();
	});

program.parse(process.argv);
