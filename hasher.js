"use strong";
"use strict";

const T = require('notmytype');
const A = require('ayy');
const utils = require('./utils');
const commaify = utils.commaify;
const compile_require = require('./compile_require');
const loadNow = utils.loadNow;
const LazyModule = utils.LazyModule;
const Transform = require('stream').Transform;

let sse4_crc32 = new LazyModule('sse4_crc32', compile_require);

// Returns [full-size blocks, remainder block]
function splitBuffer(buf, blockSize) {
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

function bufToCrc(buf) {
	return buf.readUIntBE(0, 4);
}

class CRCWriter extends Transform {
	constructor(blockSize) {
		utils.assertSafeNonNegativeInteger(blockSize);
		A.gt(blockSize, 0);
		super();
		this._blockSize = blockSize;
		this._buf = new Buffer(0);
		sse4_crc32 = loadNow(sse4_crc32);
	}

	_transform(data, encoding, callback) {
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

	_flush(callback) {
		// Need to write out the last block, even if it's under-sized
		if(this._buf.length > 0) {
			this.push(crcToBuf(sse4_crc32.calculate(this._buf)));
			this.push(this._buf);
			// Blow up if an io.js bug causes _flush to be called twice
			this._buf = null;
		}
		callback();
	}
}


class BadData extends Error {
	get name() {
		return this.constructor.name;
	}
}


const MODE_DATA = Symbol("MODE_DATA");
const MODE_CRC = Symbol("MODE_CRC");

const EMPTY_BUF = new Buffer(0);

class CRCReader extends Transform {
	constructor(blockSize) {
		utils.assertSafeNonNegativeInteger(blockSize);
		A.gt(blockSize, 0);
		super();
		this._blockSize = blockSize;
		this._counter = 0;
		this._buf = new Buffer(0);
		this._crc = null;
		this._mode = MODE_CRC;
		sse4_crc32 = loadNow(sse4_crc32);
	}

	_checkCRC(callback, actual, expect) {
		if(actual !== expect) {
			callback(new BadData(
				`CRC32C of block ${commaify(this._counter)} is allegedly \n` +
				`${crcToBuf(expect).toString('hex')} but CRC32C of data is \n` +
				`${crcToBuf(actual).toString('hex')}`)
			);
			return false;
		}
		return true;
	}

	_transform(data, encoding, callback) {
		// TODO: optimize: have a JoinedBuffer representation that doesn't need to copy
		// Alternatively, since it will almost always be data, special-case in === MODE_DATA
		data = Buffer.concat([this._buf, data]);
		this._buf = EMPTY_BUF;
		while(data.length) {
			//console.error(this._counter, data.length, this._mode);
			if(this._mode === MODE_CRC) {
				if(data.length >= 4) {
					this._crc = bufToCrc(data.slice(0, 4));
					this._mode = MODE_DATA;
					data = data.slice(4);
				} else {
					// TODO: copy to avoid leaving full data in memory?
					this._buf = data;
					data = EMPTY_BUF;
				}
			} else if(this._mode === MODE_DATA) {
				if(data.length >= this._blockSize) {
					const block = data.slice(0, this._blockSize);
					const crc = sse4_crc32.calculate(block);
					if(!this._checkCRC(callback, crc, this._crc)) {
						return;
					}
					this.push(block);
					this._counter += 1;
					this._mode = MODE_CRC;
					data = data.slice(this._blockSize);
				} else {
					// TODO: copy to avoid leaving full data in memory?
					this._buf = data;
					data = EMPTY_BUF;
				}
			}
		}
		callback();
	}

	_flush(callback) {
		// Last block might not be full-size, and now that we know we've reached
		// the end, we handle it here.
		if(!this._buf.length) {
			return;
		}
		if(this._mode === MODE_CRC) {
			callback(new BadData(`Stream ended in the middle of a CRC32C: ${this._buf.toString('hex')}`));
			return;
		}
		// It should be smaller than the block size, else it would have been handled in _transform
		A(this._buf.length < this._blockSize, this._buf.length);
		const crc = sse4_crc32.calculate(this._buf);
		if(!this._checkCRC(callback, crc, this._crc)) {
			return;
		}
		this.push(this._buf);
		// Blow up if an io.js bug causes _flush to be called twice
		this._buf = null;
		callback();
	}
}


module.exports = {BadData, CRCWriter, CRCReader};
