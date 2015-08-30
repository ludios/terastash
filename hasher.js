"use strong";
"use strict";

const T = require('notmytype');
const A = require('ayy');
const utils = require('./utils');
const compile_require = require('./compile_require');
const loadNow = utils.loadNow;
const LazyModule = utils.LazyModule;
const Transform = require('stream').Transform;

let sse4_crc32 = new LazyModule('sse4_crc32', compile_require);

// Returns [full-size blocks, remainder block]
function splitBuffer(buf, blockSize) {
	// TODO: comment for speed?
	utils.assertSafeNonNegativeInteger(blockSize);
	let start = 0;
	const bufs = [];
	while(true) {
		const block = buf.slice(start, start + blockSize);
		if(block.length < blockSize) {
			return [bufs, block];
		}
		bufs.push(block);
		start += blockSize;
	}
}

function crcToBuf(n) {
	const buf = new Buffer(4);
	buf.writeUIntBE(n, 0, 4);
	return buf;
}

class CRCWriter extends Transform {
	constructor(blockSize) {
		utils.assertSafeNonNegativeInteger(blockSize);
		super();
		this._blockSize = blockSize;
		this._buf = new Buffer(0);
		sse4_crc32 = loadNow(sse4_crc32);
	}

	_transform(data, encoding, callback) {
		//console.log(data.length);

		// Can write out at least one new block?
		if(this._buf.length + data.length >= this._blockSize) {
			// First block is special: need to include this._buf in crc32
			const firstBuf = data.slice(0, this._blockSize - this._buf.length);
			const firstCRC = sse4_crc32.calculate(
				firstBuf,
				sse4_crc32.calculate(this._buf));
			this.push(crcToBuf(firstCRC));
			this.push(this._buf);
			this.push(firstBuf);

			const _ = splitBuffer(data.slice(this._blockSize - this._buf.length), this._blockSize);
			const bufs = _[0];
			this._buf = _[1];

			for(const buf of bufs) {
				this.push(crcToBuf(sse4_crc32.calculate(buf)));
				this.push(buf);
			}
		} else {
			this._buf = Buffer.concat([this._buf, data]);
		}
		callback();
	}
}

/*
class CRCReader extends Transform {
	constructor(unpadToLength) {
		T(unpadToLength, T.number);
		A(Number.isInteger(unpadToLength), unpadToLength);
		super();
		this.bytesRead = 0;
		this._unpadToLength = unpadToLength;
	}

	_transform(data, encoding, callback) {
		// If we already read past the length we want, drop the rest of the data.
		if(this.bytesRead >= this._unpadToLength) {
			callback();
			return;
		}
		this.bytesRead += data.length;
		if(this.bytesRead <= this._unpadToLength) {
			this.push(data);
		} else {
			this.push(data.slice(0, data.length - (this.bytesRead - this._unpadToLength)));
		}
	}
}
*/

module.exports = {CRCWriter};
