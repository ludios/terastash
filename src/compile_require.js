"use strict";

const fs      = require('fs');
const path    = require('path');
const chalk   = require('chalk');
const T       = require('notmytype');
const A       = require('ayy');
const inspect = require('util').inspect;
let child_process;

/**
 * Require a module, building it first if necessary
 */
function maybeCompileAndRequire(name, verbose=false) {
	T(name, T.string, verbose, T.boolean);
	A(!name.startsWith('.'), name);
	try {
		return require(name);
	} catch(requireErr) {
		if (verbose) {
			console.error(`${name} doesn't appear to be built; building it...\n`);
		}
		let candidates;
		if (process.platform === "win32") {
			candidates = [
				// Official node.js release
				path.join(
					path.dirname(process.execPath),
					'node_modules', 'npm', 'node_modules', 'node-gyp', 'bin', 'node-gyp.js'
				),
				// From-source compile
				path.join(
					path.dirname(path.dirname(process.execPath)),
					'deps', 'npm', 'node_modules', 'node-gyp', 'bin', 'node-gyp.js'
				)
			];
		} else {
			candidates = [
				path.join(
					path.dirname(path.dirname(process.execPath)),
					'lib', 'node_modules', 'npm', 'node_modules', 'node-gyp', 'bin', 'node-gyp.js'
				)
			];
		}
		let nodeGyp;
		for (const candidate of candidates) {
			if (fs.existsSync(candidate)) {
				nodeGyp = candidate;
				break;
			}
		}
		if (!fs.existsSync(nodeGyp)) {
			throw new Error("Could not find node-gyp");
		}
		const cwd = path.join(__dirname, '../node_modules', name);
		A(fs.lstatSync(cwd).isDirectory(), `${inspect(cwd)} missing or not a directory`);
		if (!child_process) {
			child_process = require('child_process');
		}
		let child;

		child = child_process.spawnSync(
			process.execPath,
			[nodeGyp, 'clean', 'configure', 'build'],
			{
				stdio: verbose ?
					[0, 1, 2] :
					[0, 'pipe', 'pipe'],
				cwd,
				maxBuffer: 4 * 1024 * 1024
			}
		);
		if (child.status === 0) {
			return require(name);
		} else {
			console.error(chalk.bold(`\nFailed to build ${name}; you may need to install additional tools.`));
			console.error("See https://github.com/TooTallNate/node-gyp#installation");
			console.error("");
			console.error(chalk.bold("Build error was:"));
			if (child.error) {
				console.error(child.error);
			}
			if (child.stdout) {
				process.stderr.write(child.stdout);
			}
			if (child.stderr) {
				process.stderr.write(child.stderr);
			}
			console.error("");
			console.error(chalk.bold("Before building, require error was:"));
			console.error(requireErr.stack);
			console.error("");
			throw new Error(`Could not build module ${name}`);
		}
	}
}

module.exports = maybeCompileAndRequire;
