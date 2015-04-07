"use strict";

const terastash = require('.');
const ol = terastash.ol;
let program = require('commander');

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
		Name cannot be changed later.`));

program
	.command('destroy <name>')
	.description(`Destroys Cassandra keyspace ${terastash.CASSANDRA_KEYSPACE_PREFIX}<name>`);

program
	.command('add <file...>')
	.description('adds file(s) to database');

program
	.command('rm <file...>')
	.description('removes file(s) from database');

program
	.command('ls [dir...]')
	.description('list directory in the database')
	.option('-n, --name <name>', 'Ignore .terastash.json and use this stash name')
	.action(function(cmd, options) {
		//console.log({cmd, options}); process.exit();
		const name = stringOrNull(options.name);
		if(name != null && cmd[0] == null) {
			console.error("When using -n/--name, a path is required");
			process.exit(1);
		}
		terastash.lsPath(name, cmd[0] || '.');
	});

program
	.command('list-keyspaces')
	.description('list all terastash keyspaces in Cassandra');

program.parse(process.argv);

switch(program.args[0]) {
	case 'init':
		terastash.initStash(process.cwd(), program.args[1]);
		break;
	case 'destroy':
		terastash.destroyKeyspace(program.args[1]);
		break;
	case 'add':
		terastash.addFiles(program.args.slice(1));
		break;
	case 'rm':
		terastash.removeFiles(program.args.slice(1));
		break;
	case 'list-keyspaces':
		terastash.listKeyspaces();
		break;
}
