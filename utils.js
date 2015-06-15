"use strong";
"use strict";

const A = require('ayy');
const T = require('notmytype');
const Promise = require('bluebird');
const mkdirp = require('mkdirp');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const PassThrough = require('stream').PassThrough;
const basedir = require('xdg').basedir;
const compile_require = require('./compile_require');
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

/* eslint-disable no-new-func */
// Hack to allow delete in strong mode
const deleteKey = new Function("obj", "key", "delete obj[key];");
/* eslint-enable no-new-func */

const readFileAsync = Promise.promisify(fs.readFile);
const writeFileAsync = Promise.promisify(fs.writeFile);
const mkdirpAsync = Promise.promisify(mkdirp);
const statAsync = Promise.promisify(fs.stat);
const renameAsync = Promise.promisify(fs.rename);
const unlinkAsync = Promise.promisify(fs.unlink);
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

function pipeWithErrors(src, dest) {
	src.pipe(dest);
	src.once('error', function(err) {
		dest.emit('error', err);
	});
}

function makeHttpsRequest(options, stream) {
	T(options, T.object, stream, T.optional(T.shape({pipe: T.function})));
	if(!https) {
		https = require('https');
	}
	return new Promise(function(resolve, reject) {
		const req = https.request(options, resolve).once('error', function(err) {
			reject(err);
		});
		if(stream) {
			pipeWithErrors(stream, req);
		} else {
			req.end();
		}
		req.once('error', function(err) {
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

function crc32$digest(encoding) {
	const buf = new Buffer(4);
	buf.writeUIntBE(this.crc(), 0, 4);
	if(encoding === undefined) {
		return buf;
	} else {
		return buf.toString(encoding);
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
			blake2 = compile_require('blake2');
		}
		hash = blake2.createHash(algo);
	} else if(algo === "crc32c") {
		if(!sse4_crc32) {
			sse4_crc32 = compile_require('sse4_crc32');
		}
		hash = new sse4_crc32.CRC32();
		hash.digest = crc32$digest;
	} else {
		hash = crypto.createHash(algo);
	}

	const stream = new PassThrough();
	pipeWithErrors(inputStream, stream);
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

function evalMultiplications(s) {
	T(s, T.string);
	if(/^[\d\*]+$/.test(s)) {
		/* eslint-disable no-new-func */
		return new Function(`return (${s});`)();
		/* eslint-enable no-new-func */
	} else {
		throw new Error(`${s} contained something other than digits and '*'`);
	}
}

let filenameCounter = 0;
function makeChunkFilename() {
	if(Number(process.env.TERASTASH_INSECURE_AND_DETERMINISTIC)) {
		const s = `deterministic-filename-${filenameCounter}`;
		filenameCounter += 1;
		return s;
	} else {
		const seconds_s = String(Date.now()/1000).split('.')[0];
		const nanos_s = String(process.hrtime()[1]);
		const random_s = crypto.randomBytes(128/8).toString('hex');
		return `${seconds_s}-${nanos_s}-${random_s}`;
	}
}

const ChunksType = T.list(
	T.shape({
		"idx": T.number,
		"file_id": T.string,
		"crc32c": Buffer,
		"size": T.object /* Long */
	})
);

function allIdentical(arr) {
	T(arr, Array);
	for(let n=0; n < arr.length; n++) {
		if(arr[n] !== arr[0]) {
			return false;
		}
	}
	return true;
}

module.exports = {
	emptyFrozenArray, randInt, sameArrayValues, prop, shortISO, pad,
	numberWithCommas, getParentPath, getBaseName, ol,
	comparator, comparedBy, hasKey, deleteKey, readFileAsync, writeFileAsync,
	mkdirpAsync, statAsync, renameAsync, unlinkAsync, chmodAsync, utimesAsync,
	writeObjectToConfigFile, readObjectFromConfigFile, clone, makeConfigFileInitializer,
	getConcealmentSize, concealSize, pipeWithErrors, makeHttpsRequest,
	streamToBuffer, streamHasher, evalMultiplications, makeChunkFilename,
	ChunksType, allIdentical
};
