"use strict";

const T = require('notmytype');
const inspect = require('util').inspect;

class BadFilename extends Error {
	get name() {
		return this.constructor.name;
	}
}

const deviceNames = new Set();
for(const dev of
	(`CON PRN AUX NUL COM1 COM2 COM3 COM4 COM5 COM6 COM7 ` +
	`COM8 COM9 LPT1 LPT2 LPT3 LPT4 LPT5 LPT6 LPT7 LPT8 LPT9`).split(" ")) {
	deviceNames.add(dev);
}

/**
 * Checks that a unicode basename is legal on Windows, Linux, and OS X.
 * If it isn't, throws `BadFilename`.
 */
function check(s) {
	T(s, T.string);
	if(/\x00/.test(s)) {
		throw new BadFilename(`Filename cannot contain NULL; got ${inspect(s)}`);
	}
	if(/\//.test(s)) {
		throw new BadFilename(`Filename cannot contain '/'; got ${inspect(s)}`);
	}
	const trimmed = s.trim();
	if(trimmed === "" || trimmed === "." || trimmed === "..") {
		throw new BadFilename(`Trimmed filename cannot be '', '.', or '..'; got ${inspect(trimmed)} from ${inspect(s)}`);
	}
	if(/\.$/.test(s)) {
		throw new BadFilename(`Windows shell does not support filenames that end with '.'; got ${inspect(s)}`);
	}
	if(/ $/.test(s)) {
		throw new BadFilename(`Windows shell does not support filenames that end with space; got ${inspect(s)}`);
	}
	const firstPart = s.split(".")[0].toUpperCase();
	if(deviceNames.has(firstPart)) {
		throw new BadFilename(`Some Windows APIs do not support filenames ` +
			`whose non-extension component is ${inspect(firstPart)}; got ${inspect(s)}`);
	}
	if(/[|<>:"\/\\\?\*\x00-\x1F]/.test(s)) {
		throw new BadFilename(`Windows does not support filenames that contain ` +
			`\\x00-\\x1F or any of: | < > : " / \\ ? *; got ${inspect(s)}`);
	}
	if(s.length > 255) {
		throw new BadFilename(`Windows does not support filenames with > 255 characters; ${inspect(s)} has ${s.length}`);
	}
	const buf = new Buffer(s, "utf-8");
	if(buf.length > 255) {
		throw new BadFilename(`Linux does not support filenames with > 255 bytes; ${inspect(s)} has ${buf.length}`);
	}
	return s;
}

module.exports = {BadFilename, check};
