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
const PassThrough = require('stream').PassThrough;
const basedir = require('xdg').basedir;
let https;

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
	return new Promise(function(resolve) {
		let buf = new Buffer(0);
		stream.on('data', function(data) {
			buf = Buffer.concat([buf, data]);
		});
		stream.once('end', function() {
			resolve(buf);
		});
	});
}

/**
 * Require blake2, building it if necessary
 */
function requireBlake2() {
	try {
		return require('blake2');
	} catch(requireErr) {
		if(!(/^Error: Cannot find module /.test(String(requireErr)))) {
			console.error(requireErr.stack + "\n");
		}
		console.error("blake2 doesn't appear to be built; building it...\n");
		const nodeGyp = path.join(
			path.dirname(path.dirname(process.execPath)),
			'lib', 'node_modules', 'npm', 'bin', 'node-gyp-bin', 'node-gyp'
		);
		if(!fs.existsSync(nodeGyp)) {
			throw new Error("Could not find node-gyp");
		}
		const cwd = path.join(__dirname, 'node_modules', 'blake2');
		const child_process = require('child_process');
		try {
			child_process.execFileSync(
				nodeGyp,
				['clean', 'configure', 'build'],
				{stdio: [0, 1, 2], cwd}
			);
			console.error("");
		} catch(err) {
			console.error("\nBuild failed; you may need to install additional tools.  See");
			console.error("https://github.com/TooTallNate/node-gyp#installation\n");
			throw err;
		}
	}
}

let blake2;
/**
 * Take input stream, return [
 *		an output stream into which input is piped,
 *		Hash object into which input stream is piped
 * ]
 */
function streamHasher(inputStream, algo) {
	if(!blake2) {
		blake2 = requireBlake2();
	}
	const blake2b = blake2.createHash('blake2b');
	const passthrough = new PassThrough();
	inputStream.pipe(passthrough);
	passthrough.on('data', function(data) {
		blake2b.update(data);
	});
	return [passthrough, blake2b];
}

module.exports = {
	emptyFrozenArray, randInt, sameArrayValues, prop, shortISO, pad,
	numberWithCommas, getParentPath, getBaseName, catchAndLog, ol,
	comparator, comparedBy, hasKey, readFileAsync, writeFileAsync,
	mkdirpAsync, statAsync, renameAsync, writeObjectToConfigFile,
	readObjectFromConfigFile, clone, makeConfigFileInitializer,
	getConcealmentSize, concealSize, makeHttpsRequest, streamToBuffer,
	requireBlake2, streamHasher
};
