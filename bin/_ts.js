import * as terastash from '..';
import { ol } from '..';
import program from 'commander';

program.version('0.0.1');

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
	.description('adds file(s) to database')

program
	.command('rm <file...>')
	.description('removes file(s) from database')

program
	.command('ls <name> <dir...>')
	.description('list directory in the database')

program
	.command('list-keyspaces')
	.description('list all terastash keyspaces in Cassandra')

program.parse(process.argv);
//console.log(program);

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
	case 'ls':
		terastash.lsPath(program.args[1], program.args[2]);
		break;
	case 'list-keyspaces':
		terastash.listKeyspaces();
		break;
}
