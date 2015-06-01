"use strong";
"use strict";

const util = require('util');

exports.INSPECT_MAX_BYTES = 50;
exports.INSPECT_AS_STRING_LIMIT = 512;

Buffer.prototype.inspect = function inspect() {
	if(this.length > 0 && this.length <= exports.INSPECT_AS_STRING_LIMIT) {
		const decoded = this.toString('utf-8');
		// Make sure it round-trips back to the same buffer
		// before displaying it.
		if(this.equals(new Buffer(decoded, 'utf-8'))) {
			return `<${this.constructor.name} size=${this.length}: utf8 ${util.inspect(decoded)}>`;
		}
	}
	let str = '';
	const maxBytes = exports.INSPECT_MAX_BYTES;
	if(this.length > 0) {
		str = ': ' + this.toString('hex', 0, maxBytes).match(/.{2}/g).join(' ');
		if(this.length > maxBytes) {
			str += ' ... ';
		}
	}
	return `<${this.constructor.name} size=${this.length}${str}>`;
};
