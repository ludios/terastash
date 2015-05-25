"use strong";
"use strict";

const T = require('notmytype');


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

const stringOrNumType = T.union([T.string, T.number]);
function numberWithCommas(stringOrNum) {
	T(stringOrNum, stringOrNumType);
	// http://stackoverflow.com/questions/2901102/
	return ("" + stringOrNum).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

/**
 * '/'-based operation on all OSes
 */
function getParentPath(path) {
	T(path, T.string);
	const parts = path.split('/');
	parts.pop();
	return parts.join('/');
}

/**
 * '/'-based operation on all OSes
 */
function getBaseName(path) {
	T(path, T.string);
	const parts = path.split('/');
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

module.exports = {shortISO, pad, numberWithCommas, getParentPath, getBaseName, ol, comparator, comparedBy};
