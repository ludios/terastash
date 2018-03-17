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
if(!process.env.CACHE_REQUIRE_PATHS_FILE) {
	process.env.CACHE_REQUIRE_PATHS_FILE =
		basedir.configPath("terastash/internal-require-cache.json");
}
require('cache-require-paths');

require('better-buffer-inspect');

const terastash     = require('.');
const utils         = require('./utils');
const filename      = require('./filename');
const T             = require('notmytype');
const program       = require('commander');
const chalk         = require('chalk');
const Promise       = require('bluebird');
const NativePromise = global.Promise;

const EitherPromise = T.union([Promise, NativePromise]);

const ERROR_EXIT_CODE = 255;

let client;
function shutdownCassandraClient() {
	if(client) {
		try {
			client.shutdown();
		} catch(e) {
			console.error("client.shutdown() failed:");
			console.error(e.stack);
		}
	}
}

/**
 * Attaches a logging .catch to a Promise.  If an error is caught,
 * print it and exit with exit code 255.  We use 255 because we
 * frequently use ts with xargs, and 255 is the only error code that
 * makes xargs stop processing further input.
 *
 * Some known errors are handled without printing a stack trace.
 */
async function catchAndLog(p) {
	T(p, EitherPromise);
	try {
		await p;
	} catch(err) {
		if(
		err instanceof terastash.DirectoryNotEmptyError ||
		err instanceof terastash.NoSuchPathError ||
		err instanceof terastash.NotAFileError ||
		err instanceof terastash.PathAlreadyExistsError ||
		err instanceof terastash.NotInWorkingDirectoryError ||
		err instanceof terastash.KeyspaceMissingError ||
		err instanceof terastash.DifferentStashesError ||
		err instanceof terastash.UnexpectedFileError ||
		err instanceof terastash.UsageError ||
		err instanceof filename.BadFilename) {
			console.error(chalk.bold(chalk.red(err.message)));
		} else {
			console.error(err.stack);
		}
		process.exit(ERROR_EXIT_CODE);
	} finally {
		shutdownCassandraClient();
	}
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

function getOutputContext() {
	let mode;
	if(process.env.TERASTASH_OUTPUT_MODE) { // terminal, log, quiet
		mode = process.env.TERASTASH_OUTPUT_MODE;
	} else if(process.stdout.clearLine) {
		mode = 'terminal';
	} else {
		mode = 'log';
	}
	return {mode};
}

program
	.version(terastash.TERASTASH_VERSION);

program
	.command('init <name>')
	.option('-c, --chunk-store <store-name>', 'Store large files in this chunk store')
	.option('-t, --chunk-threshold <chunk-threshold>', 'If file size >= this number of bytes, put it in chunk store instead of database.  Defaults to 4096.')
	.description(d(`
		Initializes a stash in this directory and creates corresponding
		Cassandra keyspace with name ${terastash.KEYSPACE_PREFIX}<name>. Name cannot be changed later.`))
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
			options.chunkThreshold = 4096;
		}
		if(options.chunkStore === undefined) {
			console.error("-c/--chunk-store is required");
			process.exit(ERROR_EXIT_CODE);
		}
		catchAndLog(terastash.initStash(process.cwd(), name, options));
	}));

program
	.command('export-db')
	.option('-n, --name <name>', 'Use this stash name instead of inferring from current directory')
	.description(d(`
		Dump the database for this stash to stdout.  This includes content for small files stored
		in the database, but it does not retrieve content from chunk stores, merely referencing
		the chunks with pointers instead.  The dump includes all entries, even mis-parented files
		or otherwise corrupt rows.`))
	.action(a(function(options) {
		T(options, T.object);
		const name = stringOrNull(options.name);
		catchAndLog(terastash.exportDb(name));
	}));

program
	.command('import-db <dump-file>')
	.option('-n, --name <name>', 'Restore into this stash name')
	.description(d(`
		Import a database dump produced by 'ts export-db' into an already-initialized
		(but hopefully empty) stash.

		To load from stdin, use '-' for dump-file.`))
	.action(a(function(dumpFile, options) {
		T(dumpFile, T.string, options, T.object);
		const name = stringOrNull(options.name);
		if(name === null) {
			console.error("-n/--name is required");
			process.exit(ERROR_EXIT_CODE);
		}
		catchAndLog(terastash.importDb(getOutputContext(), name, dumpFile));
	}));

program
	.command('destroy <name>')
	.description(d(`
		Destroys Cassandra keyspace ${terastash.KEYSPACE_PREFIX}<name>
		and removes stash from stashes.json.  Does *not* affect the chunk store.`))
	.action(a(function(name) {
		T(name, T.string);
		catchAndLog(terastash.destroyStash(name));
	}));

/* It's 'add' instead of 'put' for left-hand-only typing */
program
	.command('add <path...>')
	.option('-d, --drop-old-if-different',
		"If path already exists in the db, and (mtime, size, executable) of " +
		"new file is different, drop the old file instead of throwing 'already exists'")
	.option('-c, --continue-on-exists', "Keep going on 'already exists' errors")
	.option('--ignore-mtime', "Ignore mtime when checking that local file is equivalent to db file")
	.description(d(`
		Add a file to the database`))
	.action(a(function(files, options) {
		T(files, T.list(T.string), options, T.object);
		catchAndLog(
			terastash.addFiles(
				getOutputContext(),
				files,
				options.continueOnExists,
				options.dropOldIfDifferent,
				/*thenShoo=*/false,
				/*justRemove=*/false,
				options.ignoreMtime
			)
		);
	}));

program
	.command('shoo <path...>')
	.option('-c, --continue-on-error', "Keep going on mtime/size mismatches and no-such-path-in-db errors")
	.option('--ignore-mtime', "Ignore mtime when checking that local file is equivalent to db file")
	.option('-r, --rm', "Don't write fakes, just remove.")
	.description(d(`
		Removes a file in the working directory and replaces it with a 'fake'
		(a zero'ed sparse file of the same length).  File must already be in the
		database.

		If mtime and size does not match between the file in the working directory
		and database, you will see an error and the working directory file will not
		be deleted.

		The 'fake' will have the sticky bit set to make it obvious that it does not
		contain real content.`))
	.action(a(function(files, options) {
		T(files, T.list(T.string), options, T.object);
		catchAndLog(terastash.shooFiles(files, options.rm, options.continueOnError, options.ignoreMtime));
	}));

program
	.command('add-shoo <path...>')
	.option('-d, --drop-old-if-different',
		"If path already exists in the db, and (mtime, size, executable) of " +
		"new file is different, drop the old file instead of throwing 'already exists'")
	.option('-r, --rm', "Don't write fakes, just remove.")
	.option('-c, --continue-on-exists', "Keep going on 'already exists' errors")
	.description(d(`
		Add a file to the database, then shoo it.`))
	.action(a(function(files, options) {
		T(files, T.list(T.string), options, T.object);
		catchAndLog(terastash.addFiles(getOutputContext(), files, options.continueOnExists, options.dropOldIfDifferent, true, options.rm));
	}));

program
	.command('get <path...>')
	.option('-n, --name <name>', 'Use this stash name instead of inferring from paths')
	.option('-f, --fake', 'Get a fake (NULLed + sticky bit) file instead of file with real content')
	.description(d(`
		Get a file or directory (recursively) from the database`))
	.action(a(function(files, options) {
		T(files, T.list(T.string), options, T.object);
		const name = stringOrNull(options.name);
		catchAndLog(terastash.getFiles(name, files, options.fake || false));
	}));

program
	.command('cat <file...>')
	.option('-n, --name <name>', 'Use this stash name instead of inferring from paths')
	.description(d(`
		Dump the contents of a file in the database to stdout`))
	.action(a(function(files, options) {
		T(files, T.list(T.string), options, T.object);
		const name = stringOrNull(options.name);
		catchAndLog(terastash.catFiles(name, files));
	}));

program
	.command('cat-ranges <fileSlashRange...>')
	.option('-n, --name <name>', 'Use this stash name instead of inferring from paths')
	.description(d(`
		Dump the contents of the specified range of each file in the database to stdout.

		For each argument, specify filename/start-end where start is the first byte
		(inclusive) and end is the last byte (exclusive).`))
	.action(a(function(args, options) {
		T(args, T.list(T.string), options, T.object);
		const name = stringOrNull(options.name);
		catchAndLog(terastash.catRangedFiles(name, args));
	}));

program
	.command('drop <file...>')
	.option('-n, --name <name>', 'Use this stash name instead of inferring from paths')
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
	.command('info <file...>')
	.option('-n, --name <name>', 'Use this stash name instead of inferring from paths')
	.option('-k, --show-keys', "Don't conceal encryption keys in the output")
	.description(d(`
		For each file, dump its database information in JSON format to stdout`))
	.action(a(function(files, options) {
		T(files, T.list(T.string), options, T.object);
		const name = stringOrNull(options.name);
		catchAndLog(terastash.infoFiles(name, files, options.showKeys || false));
	}));

program
	.command('mkdir <path...>')
	.option('-n, --name <name>', 'Use this stash name instead of inferring from paths')
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
	.option('-n, --name <name>', 'Use this stash name instead of inferring from paths')
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
	.option('-n, --name <name>', 'Use this stash name instead of inferring from paths')
	.option('-j, --just-names', 'Print just the filenames without any decoration')
	.option('-t, --sort-by-mtime', 'Sort by modification time, newest first') /* newest first to match ls behavior */
	.option('-S, --sort-by-size', 'Sort by file size, largest first') /* largest first to match ls behavior */
	.option('-r, --reverse', 'Reverse order while sorting')
	.action(a(function(paths, options) {
		T(paths, T.list(T.string), options, T.object);
		const name = stringOrNull(options.name);
		if(name !== null && !paths.length) {
			console.error("When using -n/--name, a database path is required");
			process.exit(ERROR_EXIT_CODE);
		}
		// When not using -n, and no path given, use '.'
		if(name === null && !paths.length) {
			paths[0] = '.';
		}
		catchAndLog(terastash.lsPath(
			client = terastash.getNewClient(),
			name, {
				justNames: options.justNames,
				reverse: options.reverse,
				sortByMtime: options.sortByMtime,
				sortBySize: options.sortBySize
			},
			paths[0]
		));
	}));

program
	.command('find [path...]')
	.description(d(`
		Recursively list files and directories`))
	.option('-t, --type <type>', 'Find files of this type ("f" or "d")')
	.option('-0', 'Print filenames separated by NULL instead of LF')
	.action(a(function(paths, options) {
		T(paths, T.list(T.string), options, T.object);
		const name = stringOrNull(options.name);
		if(name !== null && !paths.length) {
			console.error("When using -n/--name, a database path is required");
			process.exit(ERROR_EXIT_CODE);
		}
		// When not using -n, and no path given, use '.'
		if(name === null && !paths.length) {
			paths[0] = '.';
		}
		catchAndLog(terastash.findPath(
			name, paths[0], {print0: Boolean(options["0"]), type: options.type}
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
		terastash.checkChunkSize(options.chunkSize);
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
		terastash.checkChunkSize(options.chunkSize);
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
	.command('update-google-tokens <tokens-filename> <client-id> <client-secret>')
	.description(d(`
		Refresh the access token in tokens-filename.`))
	.action(a(function(tokensFilename, clientId, clientSecret) {
		catchAndLog(terastash.updateGoogleTokens(tokensFilename, clientId, clientSecret));
	}));

program
	.command('9p')
	.description(d(`
		Start a 9P server.

		Do *not* connect to this 9P server from a Linux 3.13 kernel (i.e., the version that
		Ubuntu 14.04 ships with.)  The kernel will lock up if the 9P server is killed during
		a read.  Linux 3.19 and 4.3-rc3 were tested to not lock up.  On Ubuntu 14.04,
		install a new-enough kernel with:

		apt-get install linux-image-generic-lts-vivid linux-headers-generic-lts-vivid
		(Linux 3.19)

		or:

		apt-get install linux-image-generic-lts-wily linux-headers-generic-lts-wily
		(Linux 4.2)
		`))
	.option('--socket <socket>', 'Create the Unix domain socket at this path')
	.action(a(function(socket) {
		if(socket === undefined) {
			console.error("--socket PATH is required");
			process.exit(ERROR_EXIT_CODE);
		}
		require('./ninep').listen(socket);
	}));

program
	.command('http')
	.description(d(`
		Start a HTTP server serving one or more stashes.
		`))
	.option('-i, --interface <interface>', 'Listen on this IP address, default 127.0.0.1')
	.option('-p, --port <interface>', 'Listen on this port')
	.option('-s, --stashes <stashes>', 'A comma-separated list of stashes to serve')
	.action(a(function(options) {
		if(options.interface === undefined) {
			options.interface = '127.0.0.1';
		}
		if(options.port === undefined) {
			console.error("http server requires -p/--port option");
			process.exit(ERROR_EXIT_CODE);
		}
		if(!options.stashes) {
			console.error("http server requires non-empty -s/--stashes option");
			process.exit(ERROR_EXIT_CODE);
		}
		require('./http').listen(options.interface, Number(options.port), options.stashes.split(','));
	}));

program
	.command('build-natives')
	.description(d(`
		Build native modules required by terastash.
		Requires a C++ compiler.`))
	.action(a(function() {
		const compile_require = require('./compile_require');
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
	console.error(chalk.bold(chalk.red(`Unknown command: ${program.args[0]}; see ts help`)));
	process.exit(ERROR_EXIT_CODE);
}
