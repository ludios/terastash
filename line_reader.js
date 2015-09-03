"use strong";
"use strict";

const T = require('notmytype');
const A = require('ayy');
const buffertools = require('buffertools');
const Transform = require('stream').Transform;


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
		const bufs = this._bufs;
		this._bufs = [];
		this.length = 0;
		return Buffer.concat(bufs);
	}
}


class DelimitedBufferDecoder extends Transform {
	constructor(delimiter) {
		T(delimiter, Buffer);
		A(delimiter.length, 1);

		// Even though we emit buffers, we obviously don't want them joined back
		// together by node streams.
		super({readableObjectMode: true});

		this._delimiter = delimiter;
		// Use JoinedBuffers because doing Buffer.concat on every _transform
		// call would exhibit O(N^2) behavior for long lines.
		this._joined = new JoinedBuffers();
	}

	_transform(data, encoding, callback) {
		T(data, Buffer);
		while(true) {
			// Search only the new data for the delimiter, not all of this._joined
			const idx = buffertools.indexOf(data, this._delimiter);
			if(idx !== -1) {
				this._joined.push(data.slice(0, idx));
				this.push(this._joined.joinPop());
				// + 1 to skip over the delimiter
				data = data.slice(idx + 1);
			} else {
				this._joined.push(data);
				break;
			}
		}
		callback();
	}

	_flush(callback) {
		T(this._joined, JoinedBuffers);
		if(this._joined.length) {
			// Need to write out the last line, even if didn't end with delimiter
			this.push(this._joined.joinPop());
		}
		this._joined = null;
		callback();
	}
}

module.exports = {DelimitedBufferDecoder};
