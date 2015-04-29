"use strict";

/**
 * ISO-ish string without the seconds
 */
function shortISO(d) {
	return d.toISOString().substr(0, 16).replace("T", " ");
}

function pad(s, wantLength) {
	return " ".repeat(Math.max(0, wantLength - s.length)) + s;
}

// http://stackoverflow.com/questions/2901102/how-to-print-a-number-with-commas-as-thousands-separators-in-javascript
function numberWithCommas(s_or_n) {
	return ("" + s_or_n).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

/**
 * '/'-based operation on all OSes
 */
function getParentPath(path) {
	const parts = path.split('/');
	parts.pop();
	return parts.join('/');
}

/**
 * '/'-based operation on all OSes
 */
function getBaseName(path) {
	const parts = path.split('/');
	return parts[parts.length - 1];
}

/**
 * Convert string with newlines and tabs to one without.
 */
function ol(s) {
	return s.replace(/[\n\t]+/g, " ");
}

module.exports = {shortISO, pad, numberWithCommas, getParentPath, getBaseName, ol};
