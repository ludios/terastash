"use strong";
"use strict";

/* eslint-disable no-process-exit */

// We use bluebird Promises in terastash, which report unhandled rejections
// without an explicit unhandledRejection handler.  But platform code or
// dependencies might use native Promises, so we need to attach this handler
// to see all unhandled rejections.
process.on("unhandledRejection", function(err) {
	const red = "\u001b[31m";
	const reset = "\u001b[39m";
	if(typeof err.stack === "string") {
		process.stderr.write(
			`${red}Unhandled rejection in a native or bluebird Promise:\n` +
			`${err.stack}${reset}\n`);
	} else {
		process.stderr.write(
			`${red}Unhandled rejection in a native or bluebird Promise:\n` +
			`${err}\n` +
			`[no stack trace available]${reset}\n`);
	}
});

// For testing the unhandledRejection handler
//new Promise(function(){ throw new Error("boom"); });
//new Promise(function(){ throw 3; });

const mkdirp = require('mkdirp');
const basedir = require('xdg').basedir;
mkdirp.sync(basedir.configPath("terastash"));
process.env.CACHE_REQUIRE_PATHS_FILE =
	basedir.configPath("terastash/internal-require-cache.json");
require('cache-require-paths');

require('better-buffer-inspect');

const terastash = require('..');
const utils = require('../utils');
const T = require('notmytype');
const program = require('commander');
const chalk = require('chalk');
const Promise = require('bluebird');
const NativePromise = global.Promise;

const EitherPromise = T.union([Promise, NativePromise]);

/**
 * Attaches a logging .catch to a Promise.  If an error is caught,
 * print it and exit with exit code 1.  Some known errors are handled
 * without printing a stack trace.
 */
function catchAndLog(p) {
	T(p, EitherPromise);
	return p.catch(function(err) {
		if(
		err instanceof terastash.DirectoryNotEmptyError ||
		err instanceof terastash.NoSuchPathError ||
		err instanceof terastash.NotAFileError ||
		err instanceof terastash.PathAlreadyExistsError ||
		err instanceof terastash.NotInWorkingDirectoryError ||
		err instanceof terastash.KeyspaceMissingError ||
		err instanceof terastash.DifferentStashesError ||
		err instanceof terastash.UnexpectedFileError) {
			console.error(chalk.bold(chalk.red(err.message)));
		} else {
			console.error(err.stack);
		}
		process.exit(1);
	});
}

// Ugly hack to avoid getting Function
function stringOrNull(o) {
	return typeof o === "string" ? o : null;
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
	return function(...args) {
		ranCommand = true;
		f.apply(this, args);
	};
}

program
	.version('0.0.1');

program
	.command('init <name>')
	.option('-c, --chunk-store <store-name>', 'Store large files in this chunk store')
	.option('-t, --chunk-threshold <chunk-threshold>', 'If file size >= this number of bytes, put it in chunk store instead of database.  Defaults to 200*1024')
	.description(d(`
		Initializes a stash in this directory and creates corresponding
		Cassandra keyspace with name ${terastash.CASSANDRA_KEYSPACE_PREFIX}<name>. Name cannot be changed later.`))
	.action(a(function(name, options) {
		T(
			name, T.string,
			options, T.shape({
				chunkStore: T.optional(T.string),
				chunkThreshold: T.optional(T.string)
			})
		);
		if(options.chunkThreshold !== undefined) {
			options.chunkThreshold = utils.evalMultiplications(options.chunkThreshold);
		} else {
			options.chunkThreshold = 200 * 1024;
		}
		if(options.chunkStore === undefined) {
			console.error("-c/--chunk-store is required");
			process.exit(1);
		}
		catchAndLog(terastash.initStash(process.cwd(), name, options));
	}));

program
	.command('dump-db')
	.option('-n, --name <name>', 'Ignore .terastash.json and use this stash name')
	.description(d(`
		Dump a backup of the database to stdout`))
	.action(a(function(options) {
		T(options, T.object);
		const name = stringOrNull(options.name);
		catchAndLog(terastash.dumpDb(name));
	}));

program
	.command('destroy <name>')
	.description(d(`
		Destroys Cassandra keyspace ${terastash.KEYSPACE_PREFIX}<name> and removes stash from stashes.json`))
	.action(a(function(name) {
		T(name, T.string);
		catchAndLog(terastash.destroyStash(name));
	}));

/* It's 'add' instead of 'put' for left-hand-only typing */
program
	.command('add <path...>')
	.description(d(`
		Add a file to the database`))
	.action(a(function(files) {
		T(files, T.list(T.string));
		catchAndLog(terastash.putFiles(files));
	}));

program
	.command('shoo <path...>')
	.description(d(`
		Removes a file in the working directory and replaces it with a 'fake'
		(a zero'ed sparse file of the same length).  File must already be in the
		database.

		If mtime and size does not match between the file in the working directory
		and database, you will see an error and the working directory file will not
		be deleted.

		The 'fake' will have the sticky bit set to make it obvious that it does not
		contain real content.`))
	.action(a(function(files) {
		T(files, T.list(T.string));
		catchAndLog(terastash.shooFiles(files));
	}));


program
	.command('get <path...>')
	.option('-n, --name <name>', 'Ignore .terastash.json and use this stash name')
	.description(d(`
		Get a file or directory (recursively) from the database`))
	.action(a(function(files, options) {
		T(files, T.list(T.string), options, T.object);
		const name = stringOrNull(options.name);
		catchAndLog(terastash.getFiles(name, files));
	}));

program
	.command('cat <file...>')
	.option('-n, --name <name>', 'Ignore .terastash.json and use this stash name')
	.description(d(`
		Dump the contents of a file in the database to stdout`))
	.action(a(function(files, options) {
		T(files, T.list(T.string), options, T.object);
		const name = stringOrNull(options.name);
		catchAndLog(terastash.catFiles(name, files));
	}));

program
	.command('drop <file...>')
	.option('-n, --name <name>', 'Ignore .terastash.json and use this stash name')
	.description(d(`
		Removes files from the database and their corresponding chunks, if any.
		Chunks in both localfs and gdrive are permanently deleted, not moved to the trash.
		This cannot be undone!

		Does not emit error or warning if specified files are not in the database.

		Does not remove the corresponding file in the working directory, if it is there.`))
	.action(a(function(files, options) {
		T(files, T.list(T.string), options, T.object);
		const name = stringOrNull(options.name);
		catchAndLog(terastash.dropFiles(name, files));
	}));

program
	.command('mkdir <path...>')
	.option('-n, --name <name>', 'Ignore .terastash.json and use this stash name')
	.description(d(`
		Creates directories in the database and in the working directory.
		Parent directories are automatically created as needed.`))
	.action(a(function(paths, options) {
		T(paths, T.list(T.string), options, T.object);
		const name = stringOrNull(options.name);
		catchAndLog(terastash.makeDirectories(name, paths));
	}));

program
	.command('mv <args...>')
	.option('-n, --name <name>', 'Ignore .terastash.json and use this stash name')
	.description(d(`
		mv src dest
		mv src1 src2 dest/

		Move a file from src to dest.  dest may be a new filename or a directory.
		If more than one src is given, dest must be a directory.
		If the corresponding file for src is in the working directory, it will be moved as well.`))
	.action(a(function(args, options) {
		T(args, T.list(T.string), options, T.object);
		const srces = args;
		const dest = srces.pop();
		const name = stringOrNull(options.name);
		catchAndLog(terastash.moveFiles(name, srces, dest));
	}));

program
	.command('ls [path...]')
	.description(d(`
		List directory in the database`))
	.option('-n, --name <name>', 'Ignore .terastash.json and use this stash name')
	.option('-j, --just-names', 'Print just the filenames without any decoration')
	.option('-t, --sort-by-mtime', 'Sort by modification time, newest first') /* newest first to match ls behavior */
	.option('-r, --reverse', 'Reverse order while sorting')
	.action(a(function(paths, options) {
		T(paths, T.list(T.string), options, T.object);
		const name = stringOrNull(options.name);
		if(name !== null && !paths.length) {
			console.error("When using -n/--name, a database path is required");
			process.exit(1);
		}
		// When not using -n, and no path given, use '.'
		if(name === null && !paths.length) {
			paths[0] = '.';
		}
		catchAndLog(terastash.lsPath(
			name, {
				justNames: options.justNames,
				reverse: options.reverse,
				sortByMtime: options.sortByMtime
			},
			paths[0]
		));
	}));

program
	.command('list-stashes')
	.description(d(`
		List all terastash keyspaces in Cassandra`))
	.action(a(function() {
		catchAndLog(terastash.listTerastashKeyspaces());
	}));

program
	.command('list-chunk-stores')
	.description(d(`
		List chunk stores`))
	.action(a(function() {
		catchAndLog(terastash.listChunkStores());
	}));

program
	.command('define-chunk-store <store-name>')
	.description(d(`
		Define a chunk store. Name cannot be changed later.`))
	.option('-t, --type <type>', 'Type of chunk store. Either localfs or gdrive.')
	.option('-s, --chunk-size <chunk-size>', 'Chunk size in bytes; must be divisble by 16.  Defaults to 1024*1024*1024.')
	.option('-d, --directory <directory>', '[localfs] Absolute path to directory to store chunks in')
	.option('--client-id <client-id>', '[gdrive] A Client ID that has Google Drive API enabled')
	.option('--client-secret <client-secret>', '[gdrive] The Client Secret corresponding to the Client ID')
	.action(a(function(storeName, options) {
		T(storeName, T.string, options, T.object);
		if(options.chunkSize !== undefined) {
			options.chunkSize = utils.evalMultiplications(options.chunkSize);
		} else {
			options.chunkSize = 1024*1024*1024;
		}
		if(options.chunkSize % 128/8 !== 0) {
			throw new Error(`Chunk size must be a multiple of 16; got ${options.chunkSize}`);
		}
		catchAndLog(terastash.defineChunkStore(storeName, options));
	}));

program
	.command('config-chunk-store <store-name>')
	.description(d(`
		Change a setting on a chunk store.`))
	.option('-t, --type <type>', 'Type of chunk store. Either localfs or gdrive.')
	.option('-s, --chunk-size <chunk-size>', 'Chunk size in bytes; must be divisible by 16.  Defaults to 1024*1024*1024.')
	.option('-d, --directory <directory>', '[localfs] Absolute path to directory to store chunks in')
	.option('--client-id <client-id>', '[gdrive] A Client ID that has Google Drive API enabled')
	.option('--client-secret <client-secret>', '[gdrive] The Client Secret corresponding to the Client ID')
	.action(a(function(storeName, options) {
		T(storeName, T.string, options, T.object);
		if(options.chunkSize !== undefined) {
			options.chunkSize = utils.evalMultiplications(options.chunkSize);
		}
		if(options.chunkSize % 128/8 !== 0) {
			throw new Error(`Chunk size must be a multiple of 16; got ${options.chunkSize}`);
		}
		catchAndLog(terastash.configChunkStore(storeName, options));
	}));

program
	.command('auth-gdrive <store-name>')
	.description(d(`
		For gdrive chunk store <store-name>, start the OAuth2 authorization flow.`))
	.action(a(function(storeName) {
		T(storeName, T.string);
		catchAndLog(terastash.authorizeGDrive(storeName));
	}));

program
	.command('build-natives')
	.description(d(`
		Build native modules required by terastash.
		Requires a C++ compiler.`))
	.action(a(function() {
		const compile_require = require('../compile_require');
		compile_require('blake2');
		compile_require('sse4_crc32');
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
	console.log(chalk.bold(chalk.red(`Unknown command: ${program.args[0]}; see ts help`)));
	process.exit(1);
}
