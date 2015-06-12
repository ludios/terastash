"use strong";
"use strict";

const A = require('ayy');
const T = require('notmytype');
const NativePromise = global.Promise;
const Promise = require('bluebird');
const mkdirp = require('mkdirp');
const fs = require('fs');
const path = require('path');
const process = require('process');
const crypto = require('crypto');
const PassThrough = require('stream').PassThrough;
const basedir = require('xdg').basedir;
const chalk = require('chalk');
let https;
let blake2;
let sse4_crc32;

const emptyFrozenArray = [];
Object.freeze(emptyFrozenArray);

function randInt(min, max) {
	const range = max - min;
	const rand = Math.floor(Math.random() * (range + 1));
	return min + rand;
}

/**
 * Returns a function that gets the given property on any object passed in
 */
function prop(name) {
	return function(obj) {
		return obj[name];
	};
}

function sameArrayValues(arr1, arr2) {
	T(arr1, Array, arr2, Array);
	const length = arr1.length;
	if(length !== arr2.length) {
		return false;
	}
	for(let i=0; i < length; i++) {
		if(!Object.is(arr1[i], arr2[i])) {
			return false;
		}
	}
	return true;
}

/**
 * ISO-ish string without the seconds
 */
function shortISO(d) {
	T(d, Date);
	return d.toISOString().substr(0, 16).replace("T", " ");
}

function pad(s, wantLength) {
	T(s, T.string, wantLength, T.number);
	return " ".repeat(Math.max(0, wantLength - s.length)) + s;
}

const StringOrNumber = T.union([T.string, T.number]);
function numberWithCommas(stringOrNum) {
	T(stringOrNum, StringOrNumber);
	// http://stackoverflow.com/questions/2901102/
	return ("" + stringOrNum).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

/**
 * '/'-based operation on all OSes
 */
function getParentPath(p) {
	T(p, T.string);
	const parts = p.split('/');
	parts.pop();
	return parts.join('/');
}

/**
 * '/'-based operation on all OSes
 */
function getBaseName(p) {
	T(p, T.string);
	const parts = p.split('/');
	return parts[parts.length - 1];
}

/**
 * Convert string with newlines and tabs to one without.
 */
function ol(s) {
	T(s, T.string);
	return s.replace(/[\n\t]+/g, " ");
}

/**
 * Takes a predicate function that returns true if x < y and returns a
 * comparator function that can be passed to arr.sort(...)
 *
 * Like clojure.core/comparator
 */
function comparator(pred) {
	T(pred, T.function);
	return function(x, y) {
		if(pred(x, y)) {
			return -1;
		} else if(pred(y, x)) {
			return 1;
		} else {
			return 0;
		}
	};
}

/**
 * Takes a function that maps obj -> (key to sort by) and
 * returns a comparator function that can be passed to arr.sort(...)
 */
function comparedBy(mapping, reverse) {
	T(mapping, T.function, reverse, T.optional(T.boolean));
	if(!reverse) {
		return comparator(function(x, y) {
			return mapping(x) < mapping(y);
		});
	} else {
		return comparator(function(x, y) {
			return mapping(x) > mapping(y);
		});
	}
}

function hasKey(obj, key) {
	T(obj, T.object, key, T.string);
	return Object.prototype.hasOwnProperty.call(obj, key);
}

const EitherPromise = T.union([Promise, NativePromise]);

/**
 * Attaches a logging .catch to a Promise
 */
function catchAndLog(p) {
	T(p, EitherPromise);
	return p.catch(function(err) {
		console.error(err.stack);
	});
}

const readFileAsync = Promise.promisify(fs.readFile);
const writeFileAsync = Promise.promisify(fs.writeFile);
const mkdirpAsync = Promise.promisify(mkdirp);
const statAsync = Promise.promisify(fs.stat);
const renameAsync = Promise.promisify(fs.rename);
const chmodAsync = Promise.promisify(fs.chmod);
const utimesAsync = Promise.promisify(fs.utimes);

const writeObjectToConfigFile = Promise.coroutine(function*(fname, object) {
	T(fname, T.string, object, T.object);
	const configPath = basedir.configPath(path.join("terastash", fname));
	yield mkdirpAsync(path.dirname(configPath));
	yield writeFileAsync(configPath, JSON.stringify(object, null, 2));
});

const readObjectFromConfigFile = Promise.coroutine(function*(fname) {
	T(fname, T.string);
	const configPath = basedir.configPath(path.join("terastash", fname));
	const buf = yield readFileAsync(configPath);
	return JSON.parse(buf);
});

// Beware: clone converts undefined to null
function clone(obj) {
	return JSON.parse(JSON.stringify(obj));
}

const makeConfigFileInitializer = function(fname, defaultConfig) {
	T(fname, T.string, defaultConfig, T.object);
	return Promise.coroutine(function*() {
		try {
			return (yield readObjectFromConfigFile(fname));
		} catch(e) {
			if(e.code !== 'ENOENT') {
				throw e;
			}
			// If there is no config file, write defaultConfig.
			yield writeObjectToConfigFile(fname, defaultConfig);
			return clone(defaultConfig);
		}
	});
};

function roundUpToNearest(n, nearest) {
	T(n, T.number, nearest, T.number);
	A(Number.isInteger(n), n);
	A(Number.isInteger(nearest), nearest);
	return Math.ceil(n/nearest) * nearest;
}

/**
 * For tiny files (< 2KB), return 16
 * For non-tiny files, return (2^floor(log2(n)))/64
 */
function getConcealmentSize(n) {
	T(n, T.number);
	A(Number.isInteger(n), n);
	const averageWasteage = 1/128; // ~= .78%
	let ret = Math.pow(2, Math.floor(Math.log2(n))) * (averageWasteage*2);
	// This also takes care of non-integers we get out of the above fn
	ret = Math.max(16, ret);
	A(Number.isInteger(ret), ret);
	return ret;
}

/**
 * Conceal a file size by rounding the size up log2-proportionally,
 * to a size 0% to 1.5625% of the original size.
 */
function concealSize(n) {
	T(n, T.number);
	A(Number.isInteger(n), n);
	const ret = roundUpToNearest(Math.max(1, n), getConcealmentSize(n));
	A.gte(ret, n);
	return ret;
}

function makeHttpsRequest(options) {
	T(options, T.object);
	if(!https) {
		https = require('https');
	}
	return new Promise(function(resolve, reject) {
		https.get(options, resolve).on('error', function(err) {
			reject(err);
		});
	});
}

function streamToBuffer(stream) {
	T(stream, T.shape({on: T.function, once: T.function, resume: T.function}));
	return new Promise(function(resolve, reject) {
		let buf = new Buffer(0);
		stream.on('data', function(data) {
			buf = Buffer.concat([buf, data]);
		});
		stream.once('end', function() {
			resolve(buf);
		});
		stream.once('error', function(err) {
			reject(err);
		});
		stream.resume();
	});
}

/**
 * Require a module, building it first if necessary
 */
function maybeCompileAndRequire(name, verbose) {
	T(name, T.string, verbose, T.optional(T.boolean));
	try {
		return require(name);
	} catch(requireErr) {
		if(verbose) {
			console.error(`${name} doesn't appear to be built; building it...\n`);
		}
		const nodeGyp = path.join(
			path.dirname(path.dirname(process.execPath)),
			'lib', 'node_modules', 'npm', 'bin', 'node-gyp-bin', 'node-gyp'
		);
		if(!fs.existsSync(nodeGyp)) {
			throw new Error("Could not find node-gyp");
		}
		const cwd = path.join(__dirname, 'node_modules', name);
		const child_process = require('child_process');
		let child;

		child = child_process.spawnSync(
			nodeGyp,
			['clean', 'configure', 'build'],
			{
				stdio: verbose ?
					[0, 1, 2] :
					[0, 'pipe', 'pipe'],
				cwd,
				maxBuffer: 4*1024*1024
			}
		);
		if(child.status === 0) {
			return require(name);
		} else {
			console.error(chalk.bold(`\nFailed to build ${name}; you may need to install additional tools.`));
			console.error("See https://github.com/TooTallNate/node-gyp#installation");
			console.error("");
			console.error(chalk.bold("Build error was:"));
			process.stderr.write(child.stdout);
			process.stderr.write(child.stderr);
			console.error("");
			console.error(chalk.bold("Before building, require error was:"));
			console.error(requireErr.stack);
			console.error("");
			throw new Error(`Could not build module ${name}`);
		}
	}
}

/**
 * Take input stream, return {
 *		stream: an output stream into which input is piped,
 *		hash: Hash object that hashes input stream as it is read,
 *		length: number of bytes read from input stream
 * }
 */
function streamHasher(inputStream, algo) {
	T(inputStream, T.shape({pipe: T.function}), algo, T.string);
	let hash;
	if(/^blake2/.test(algo)) {
		if(!blake2) {
			blake2 = maybeCompileAndRequire('blake2');
		}
		hash = blake2.createHash(algo);
	} else if(algo === "crc32c") {
		if(!sse4_crc32) {
			sse4_crc32 = maybeCompileAndRequire('sse4_crc32');
		}
		hash = new sse4_crc32.CRC32();
	} else {
		hash = crypto.createHash(algo);
	}

	const stream = new PassThrough();
	inputStream.pipe(stream);
	const out = {stream, hash, length: 0};
	stream.on('data', function(data) {
		out.length += data.length;
		hash.update(data);
	});
	// We attached a 'data' handler, but don't let that put us into
	// flowing mode yet, because the user hasn't attached their own
	// 'data' handler yet.
	stream.pause();
	return out;
}

module.exports = {
	emptyFrozenArray, randInt, sameArrayValues, prop, shortISO, pad,
	numberWithCommas, getParentPath, getBaseName, catchAndLog, ol,
	comparator, comparedBy, hasKey, readFileAsync, writeFileAsync,
	mkdirpAsync, statAsync, renameAsync, chmodAsync, utimesAsync,
	writeObjectToConfigFile, readObjectFromConfigFile, clone, makeConfigFileInitializer,
	getConcealmentSize, concealSize, makeHttpsRequest, streamToBuffer,
	maybeCompileAndRequire, streamHasher
};
