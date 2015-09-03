/**
 * Like streamifier, but return 64KB at a time instead of the entire
 * Buffer in the first _read call.
 */

"use strong";
"use strict";

const util = require('util');
const T = require('notmytype');
const Readable = require('stream').Readable;

class MultiStream extends Readable {
	constructor(buf, options) {
		T(buf, Buffer, options, T.optional(T.object));
		options = options || {};
		super({
			highWaterMark: options.highWaterMark,
			encoding: options.encoding
		});
		this._buf = buf;
		this._idx = 0;
		this._readSize = 64 * 1024;
	}

	_read() {
		T(this._buf, Buffer);
		const slice = this._buf.slice(this._idx, this._idx + this._readSize);
		this._idx += this._readSize;
		if(slice.length) {
			this.push(slice);
		} else {
			this.push(null);
			this._buf = null;
		}
	}
}

module.exports.createReadStream = function(buf, options) {
	return new MultiStream(buf, options);
};
