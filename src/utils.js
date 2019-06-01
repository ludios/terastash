"use strict";

const A               = require('ayy');
const T               = require('notmytype');
const fs              = require('./fs-promisified');
const path            = require('path');
const crypto          = require('crypto');
const PassThrough     = require('stream').PassThrough;
const basedir         = require('xdg').basedir;
const inspect         = require('util').inspect;
const mkdirp          = require('mkdirp');
const https           = require('https');
const dgram           = require("dgram");
const compile_require = require('./compile_require');
const sse4_crc32      = compile_require('sse4_crc32');

const OutputContextType = T.shape({mode: T.string});

function assertSafeNonNegativeInteger(num) {
	T(num, T.number);
	A(Number.isInteger(num), num);
	A.gte(num, 0);
	A.lte(num, Number.MAX_SAFE_INTEGER);
}

function assertSafeNonNegativeLong(long) {
	A(long.greaterThanOrEqual(0), long);
	A(long.lessThanOrEqual(Number.MAX_SAFE_INTEGER), long);
}

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
	if (length !== arr2.length) {
		return false;
	}
	for (let i=0; i < length; i++) {
		if (!Object.is(arr1[i], arr2[i])) {
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
	assertSafeNonNegativeInteger(wantLength);
	return " ".repeat(Math.max(0, wantLength - s.length)) + s;
}

const StringOrNumber = T.union([T.string, T.number]);
function commaify(stringOrNum) {
	T(stringOrNum, StringOrNumber);
	// http://stackoverflow.com/questions/2901102/
	return String(stringOrNum).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
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
		if (pred(x, y)) {
			return -1;
		} else if (pred(y, x)) {
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
function comparedBy(mapping, reverse=false) {
	T(mapping, T.function, reverse, T.boolean);
	if (!reverse) {
		return comparator(function(x, y) {
			return mapping(x) < mapping(y);
		});
	} else {
		return comparator(function(x, y) {
			return mapping(x) > mapping(y);
		});
	}
}

function mkdirpAsync(dir, opts) {
	return new Promise((resolve, reject) => {
		mkdirp(dir, opts, (err, made) => err === null ? resolve(made) : reject(err));
	});
}

async function writeObjectToConfigFile(fname, object) {
	T(fname, T.string, object, T.object);
	const configPath = basedir.configPath(path.join("terastash", fname));
	await mkdirpAsync(path.dirname(configPath));
	await fs.writeFileAsync(configPath, JSON.stringify(object, null, 2));
}

async function readObjectFromConfigFile(fname) {
	T(fname, T.string);
	const configPath = basedir.configPath(path.join("terastash", fname));
	const buf = await fs.readFileAsync(configPath);
	return JSON.parse(buf);
}

// Beware: clone converts undefined to null
function clone(obj) {
	return JSON.parse(JSON.stringify(obj));
}

function makeConfigFileInitializer(fname, defaultConfig) {
	T(fname, T.string, defaultConfig, T.object);
	return async function makeConfigFileInitializer$coro() {
		try {
			return (await readObjectFromConfigFile(fname));
		} catch(err) {
			if (err.code !== 'ENOENT') {
				throw err;
			}
			// If there is no config file, write defaultConfig.
			await writeObjectToConfigFile(fname, defaultConfig);
			return clone(defaultConfig);
		}
	};
}

function roundUpToNearest(n, nearest) {
	T(n, T.number, nearest, T.number);
	assertSafeNonNegativeInteger(n);
	assertSafeNonNegativeInteger(nearest);
	return Math.ceil(n/nearest) * nearest;
}

/**
 * For tiny files (< 2KB), return 16
 * For non-tiny files, return (2^floor(log2(n)))/64
 */
function getConcealmentSize(n) {
	assertSafeNonNegativeInteger(n);
	const averageWasteage = 1/128; // ~= .78%
	let ret = Math.pow(2, Math.floor(Math.log2(n))) * (averageWasteage*2);
	// This also takes care of non-integers we get out of the above fn
	ret = Math.max(16, ret);
	assertSafeNonNegativeInteger(ret);
	return ret;
}

/**
 * Conceal a file size by rounding the size up log2-proportionally,
 * to a size 0% to 1.5625% of the original size.
 */
function concealSize(n) {
	assertSafeNonNegativeInteger(n);
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

const StreamType = T.shape({
	read: T.function,
	pipe: T.function,
	on: T.function,
	once: T.function,
	pause: T.function,
	resume: T.function
});

function makeHttpsRequest(options, stream) {
	T(options, T.object, stream, T.optional(StreamType));
	return new Promise(function makeHttpsRequest$Promise(resolve, reject) {
		const req = https.request(options, resolve).once('error', function(err) {
			reject(err);
		});
		if (stream) {
			pipeWithErrors(stream, req);
		} else {
			req.end();
		}
		req.once('error', function(err) {
			reject(err);
		});
	});
}

function readableToBuffer(stream) {
	T(stream, StreamType);
	return new Promise(function readableToBuffer$Promise(resolve, reject) {
		const bufs = [];
		stream.on('data', function(data) {
			bufs.push(data);
		});
		stream.once('end', function() {
			resolve(Buffer.concat(bufs));
		});
		stream.once('error', function(err) {
			reject(err);
		});
		stream.resume();
	});
}

function writableToBuffer(stream) {
	T(stream, StreamType);
	return new Promise(function writableToBuffer$Promise(resolve, reject) {
		const bufs = [];
		stream.on('data', function(data) {
			bufs.push(data);
		});
		stream.once('finish', function() {
			resolve(Buffer.concat(bufs));
		});
		stream.once('error', function(err) {
			reject(err);
		});
		stream.resume();
	});
}

class CRC32C {
	constructor() {
		this.crc = 0;
	}

	update(buf) {
		T(buf, Buffer);
		this.crc = sse4_crc32.calculate(buf, this.crc);
	}

	digest(encoding) {
		T(encoding, T.optional(T.string));
		const buf = Buffer.allocUnsafe(4);
		buf.writeUIntBE(this.crc, 0, 4);
		if (encoding === undefined) {
			return buf;
		} else {
			return buf.toString(encoding);
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
function streamHasher(inputStream, algoOrExistingHash, existingLength=0) {
	T(
		inputStream, StreamType,
		algoOrExistingHash, T.union([T.string, T.object]),
		existingLength, T.number
	);
	assertSafeNonNegativeInteger(existingLength);
	let hash;
	if (typeof algoOrExistingHash === "string") {
		const algo = algoOrExistingHash;
		if (algo === "crc32c") {
			hash = new CRC32C();
		} else {
			hash = crypto.createHash(algo);
		}
	} else {
		hash = algoOrExistingHash;
	}

	const stream = new PassThrough();
	stream.destroy = function streamHasher$stream$destroy() {
		inputStream.destroy();
	};
	pipeWithErrors(inputStream, stream);
	const out = {stream, hash, length: existingLength};
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
	if (/^[ \d\*]+$/.test(s)) {
		/* eslint-disable no-new-func */
		return new Function(`return (${s});`)();
		/* eslint-enable no-new-func */
	} else {
		throw new Error(`${s} contained something other than space, digits, and '*'`);
	}
}

function dateNow() {
	if (Number(process.env.TERASTASH_INSECURE_AND_DETERMINISTIC)) {
		return new Date(0);
	} else {
		return new Date();
	}
}

let filenameCounter = 0;
function makeChunkFilename() {
	if (Number(process.env.TERASTASH_INSECURE_AND_DETERMINISTIC)) {
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
		"size": T.number
	})
);

function allIdentical(arr) {
	T(arr, Array);
	for (let n=0; n < arr.length; n++) {
		if (arr[n] !== arr[0]) {
			return false;
		}
	}
	return true;
}

function filledArray(n, obj) {
	T(n, T.number, obj, T.any);
	assertSafeNonNegativeInteger(n);
	return Array.apply(null, new Array(n)).map(function() { return obj; });
}

class PersistentCounter {
	constructor(fname, start=0) {
		T(fname, T.string, start, T.number);
		this.fname = fname;
		assertSafeNonNegativeInteger(start);
		this.start = start;
	}

	getNext() {
		let n;
		try {
			n = Number(fs.readFileSync(this.fname));
		} catch(err) {
			if (err.code !== 'ENOENT') {
				throw err;
			}
			n = this.start;
		}
		assertSafeNonNegativeInteger(n);
		this.setNumber(n + 1);
		return n;
	}

	setNumber(num) {
		assertSafeNonNegativeInteger(num);
		return Number(fs.writeFileSync(this.fname, String(num)));
	}

	inspect() {
		return `<PersistentCounter fname=${inspect(this.fname)}>`;
	}
}

const WILDCARD = Symbol('WILDCARD');
const ColsType = T.list(T.union([T.string, T.symbol]));

function colsAsString(cols) {
	T(cols, ColsType);
	// TODO: validate cols for lack of injection?
	return cols.map(function(k) {
		if (k === WILDCARD) {
			return "*";
		} else {
			return JSON.stringify(k);
		}
	}).join(", ");
}

const NumberOrDateType = T.union([T.number, Date]);

/**
 * Like fs.utimes, but preserves milliseconds by getting a file handle and
 * using futimes
 * https://github.com/joyent/libuv/issues/1371
 * https://github.com/joyent/node/issues/7000#issuecomment-33758278
 */
async function utimesMilliseconds(fname, atime, mtime) {
	T(fname, T.string, atime, NumberOrDateType, mtime, NumberOrDateType);
	const outputHandle = await fs.openAsync(fname, "r");
	try {
		// Use fs.futimes instead of fs.utimes because fs.utimes has
		// 1-second granularity, losing the milliseconds.
		await fs.futimesAsync(outputHandle, atime, mtime);
	} finally {
		await fs.closeAsync(outputHandle);
	}
}

async function tryUnlink(fname) {
	try {
		await fs.unlinkAsync(fname);
	} catch(err) {
		if (err.code !== "ENOENT") {
			throw err;
		}
		// else, ignore error
	}
}


const EMPTY_BUF = Buffer.alloc(0);

/**
 * An object that holds multiple Buffers and knows the total
 * length, allowing you to delay the .concat() until you need
 * the whole thing.
 */
class JoinedBuffers {
	constructor() {
		this._bufs = [];
		this.length = 0;
	}

	push(buf) {
		T(buf, Buffer);
		this.length += buf.length;
		this._bufs.push(buf);
	}

	joinPop() {
		if (!this._bufs.length) {
			return EMPTY_BUF;
		}
		const bufs = this._bufs;
		this._bufs = [];
		this.length = 0;
		if (bufs.length === 1) {
			return bufs[0];
		} else {
			return Buffer.concat(bufs);
		}
	}
}

function clearOrLF(stdStream) {
	if (stdStream.clearLine && stdStream.cursorTo) {
		stdStream.clearLine();
		stdStream.cursorTo(0);
	} else {
		stdStream.write('\n');
	}
}

function pluralize(count, singular, plural) {
	T(count, T.number, singular, T.string, plural, T.string);
	return `${commaify(count)} ${count === 1 ? singular : plural}`;
}

// Returns [full-size blocks, remainder block]
function splitBuffer(buf, blockSize) {
	let start = 0;
	const bufs = [];
	while (true) {
		const block = buf.slice(start, start + blockSize);
		if (block.length < blockSize) {
			return [bufs, block];
		}
		bufs.push(block);
		start += blockSize;
	}
}


/**
 * Like Python's s.split(delim, num) and s.split(delim)
 * This does *NOT* implement Python's no-argument s.split()
 *
 * @param {string} s The string to split.
 * @param {string} sep The separator to split by.
 * @param {number} maxsplit Maximum number of times to split.
 *
 * @return {!Array.<string>} The splitted string, as an array.
 */
function splitString(s, sep, maxsplit) {
	T(s, T.string, sep, T.string, maxsplit, T.optional(T.number));
	if (maxsplit === undefined || maxsplit < 0) {
		return s.split(sep);
	}
	const pieces = s.split(sep);
	const head = pieces.splice(0, maxsplit);
	// after the splice, pieces is shorter and no longer has the `head` elements.
	if (pieces.length > 0) {
		const tail = pieces.join(sep);
		head.push(tail); // no longer just the head.
	}
	return head;
}

/**
 * Like Python's s.rsplit(delim, num) and s.rsplit(delim)
 * This does *NOT* implement Python's no-argument s.rsplit()
 *
 * @param {string} s The string to rsplit.
 * @param {string} sep The separator to rsplit by.
 * @param {number} maxsplit Maximum number of times to rsplit.
 *
 * @return {!Array.<string>} The rsplitted string, as an array.
 */
function rsplitString(s, sep, maxsplit) {
	T(s, T.string, sep, T.string, maxsplit, T.optional(T.number));
	if (maxsplit === undefined || maxsplit < 0) {
		return s.split(sep);
	}
	const pieces = s.split(sep);
	const tail = pieces.splice(pieces.length - maxsplit, pieces.length);
	// after the splice, pieces is shorter and no longer has the C{tail} elements.
	if (pieces.length > 0) {
		const head = pieces.join(sep);
		tail.splice(0, 0, head); // no longer just the tail.
	}
	return tail;
}

const RangeType = T.tuple([T.number, T.number]);
const RangesType = T.list(RangeType);

function checkRange(range) {
	T(range, RangeType);
	assertSafeNonNegativeInteger(range[0]);
	assertSafeNonNegativeInteger(range[1]);
	A.lt(range[0], range[1]);
}

function intersect(range1, range2) {
	checkRange(range1);
	checkRange(range2);
	// Range is the max of the beginnings to the min of the ends
	const start = Math.max(range1[0], range2[0]);
	const end = Math.min(range1[1], range2[1]);
	if (!(start < end)) {
		return null;
	}
	return [start, end];
}

function* zip(...iterables) {
	T(iterables, T.list(T.any));
	if (!iterables.length) {
		return;
	}
	iterables = iterables.map(iterable => iterable[Symbol.iterator]());
	const rest = iterables.slice(1);
	for (const item of iterables[0]) {
		yield [item].concat(rest.map(iterable => iterable.next().value));
	}
}

function statsdSend(s) {
	const host    = "127.0.0.1";
	const port    = 8125;
	const client  = dgram.createSocket("udp4");
	const message = new Buffer.from(s);
	return new Promise(function(resolve, reject) {
		client.send(message, 0, message.length, port, host, function(err, _bytes) {
			try {
				client.close();
			} catch(_) {}

			if (err) {
				reject(err);
			} else {
				resolve();
			}
		});
	});
}

function statsdCounterIncrement(name, value, tags) {
	let s = `${name}:${value}|c`;
	if (tags) {
		s += "|#";
		s += tags.join(",");
	}
	return statsdSend(s);
}

module.exports = {
	OutputContextType, assertSafeNonNegativeInteger, assertSafeNonNegativeLong,
	randInt, sameArrayValues, prop, shortISO, pad, commaify, getParentPath,
	getBaseName, ol, comparator, comparedBy, mkdirpAsync,

	writeObjectToConfigFile, readObjectFromConfigFile, clone, dateNow,
	makeConfigFileInitializer, getConcealmentSize, concealSize, pipeWithErrors,
	makeHttpsRequest, readableToBuffer, writableToBuffer, streamHasher, evalMultiplications,
	makeChunkFilename, StreamType, ChunksType, allIdentical, filledArray,
	PersistentCounter, WILDCARD, colsAsString, ColsType, utimesMilliseconds,
	tryUnlink, JoinedBuffers, clearOrLF, pluralize, splitBuffer,
	splitString, rsplitString, RangeType, RangesType, checkRange, intersect, zip,
	statsdCounterIncrement
};
