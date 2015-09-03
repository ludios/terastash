"use strong";
"use strict";

const T = require('notmytype');
const A = require('ayy');
const buffertools = require('buffertools');
const Transform = require('stream').Transform;


const EMPTY_BUF = new Buffer(0);

class DelimitedBufferDecoder extends Transform {
	constructor(delimiter) {
		T(delimiter, Buffer);
		A(delimiter.length, 1);

		// Even though we emit buffers, we obviously don't want them joined back
		// together by node streams.
		super({readableObjectMode: true});
		this._delimiter = delimiter;
		this._buf = EMPTY_BUF;
	}

	_transform(data, encoding, callback) {
		T(data, Buffer);
		data = Buffer.concat([this._buf, data]);
		this._buf = EMPTY_BUF;
		while(true) {
			const idx = buffertools.indexOf(data, this._delimiter);
			if(idx !== -1) {
				this.push(data.slice(0, idx));
				// + 1 to skip over the delimiter
				data = data.slice(idx + 1);
			} else {
				this._buf = data;
				break;
			}
		}
		callback();
	}

	_flush(callback) {
		T(this._buf, Buffer);
		if(this._buf.length) {
			// Need to write out the last line, even if didn't end with delimiter
			this.push(this._buf);
		}
		this._buf = null;
		callback();
	}
}

module.exports = {DelimitedBufferDecoder};
