"use strong";
"use strict";

const T = require('notmytype');
const inspect = require('util').inspect;

class BadFilename extends Error {
	get name() {
		return this.constructor.name;
	}
}

const deviceNames = Object.create(null);
for(const dev of
	`CON PRN AUX NUL COM1 COM2 COM3 COM4 COM5 COM6 COM7 ` +
	`COM8 COM9 LPT1 LPT2 LPT3 LPT4 LPT5 LPT6 LPT7 LPT8 LPT9`.split(" ")) {
	deviceNames[dev] = true;
}

/**
 * Checks that a basename is legal on both Windows and Linux.
 * If it isn't, throws `BadFilename`
 */
function check(s) {
	T(s, T.string);
	if(/\.$/.test(s)) {
		throw new BadFilename(`Windows shell does not support filenames that end with '.'; got ${inspect(s)}`);
	}
	if(/ $/.test(s)) {
		throw new BadFilename(`Windows shell does not support filenames that end with space; got ${inspect(s)}`);
	}
	const firstPart = s.split(".")[0].toUpperCase();
	if(deviceNames[firstPart] === true) {
		throw new BadFilename(`Some Windows APIs do not support filenames ` +
			`whose non-extension component is ${inspect(firstPart)}; got ${inspect(s)}`);
	}
	if(/[|<>:"\/\\\?\*\x00-\x1F]/.test(s)) {
		throw new BadFilename(`Windows does not support filenames that contain ` +
			`\\x00-\\x1F or any of: | < > : " / \\ ? *; got ${inspect(s)}`);
	}
	if(s.length > 255) {
		throw new BadFilename(`Windows does not support filenames with > 255 characters`);
	}
}

module.exports = {check};
