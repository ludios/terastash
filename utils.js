"use strong";
"use strict";

const T = require('notmytype');
const NativePromise = global.Promise;
const Promise = require('bluebird');
const mkdirp = require('mkdirp');
const fs = require('fs');
const path = require('path');
const basedir = require('xdg').basedir;

const emptyFrozenArray = [];
Object.freeze(emptyFrozenArray);

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

// Beware: undefined converted to null
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

module.exports = {
	emptyFrozenArray, shortISO, pad, numberWithCommas, getParentPath, getBaseName,
	catchAndLog, ol, comparator, comparedBy, hasKey,
	readFileAsync, writeFileAsync, mkdirpAsync, statAsync,
	writeObjectToConfigFile, readObjectFromConfigFile, clone,
	makeConfigFileInitializer
};
