"use strict";

const A = require('ayy');
const utils = require('./utils');
const commaify = utils.commaify;
const compile_require = require('./compile_require');
const loadNow = utils.loadNow;
const LazyModule = utils.LazyModule;
const JoinedBuffers = utils.JoinedBuffers;
const Transform = require('stream').Transform;

let sse4_crc32 = new LazyModule('sse4_crc32', compile_require);

function crcToBuf(n) {
	const buf = Buffer.allocUnsafe(4);
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
		this._joined = new JoinedBuffers();
		sse4_crc32 = loadNow(sse4_crc32);
	}

	_pushCRCAndBuf(buf) {
		this.push(crcToBuf(sse4_crc32.calculate(buf)));
		this.push(buf);
	}

	_transform(data, encoding, callback) {
		this._joined.push(data);
		// Can write out at least one new block?
		if(this._joined.length >= this._blockSize) {
			const [splitBufs, remainder] = utils.splitBuffer(this._joined.joinPop(), this._blockSize);
			this._joined.push(remainder);

			for(const buf of splitBufs) {
				this._pushCRCAndBuf(buf);
			}
		}
		callback();
	}

	_flush(callback) {
		// Need to write out the last block, even if it's under-sized
		if(this._joined.length > 0) {
			const buf = this._joined.joinPop();
			this._pushCRCAndBuf(buf);
			this._joined = null;
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

const EMPTY_BUF = Buffer.alloc(0);

class CRCReader extends Transform {
	constructor(blockSize) {
		utils.assertSafeNonNegativeInteger(blockSize);
		A.gt(blockSize, 0);
		super();
		this._blockSize = blockSize;
		this._counter = 0;
		this._joined = new JoinedBuffers();
		this._crc = null;
		this._mode = MODE_CRC;
		sse4_crc32 = loadNow(sse4_crc32);
	}

	_checkCRC(callback, actual, expect) {
		if(actual !== expect) {
			callback(new BadData(
				`CRC32C of block ${commaify(this._counter)} is allegedly\n` +
				`${crcToBuf(expect).toString('hex')} but CRC32C of data is\n` +
				`${crcToBuf(actual).toString('hex')}`)
			);
			return false;
		}
		return true;
	}

	_transform(newData, encoding, callback) {
		this._joined.push(newData);
		// Don't bother processing anything if we don't have enough to decode
		// a CRC or a block.
		if(this._mode === MODE_CRC && this._joined.length < 4 ||
		this._mode === MODE_DATA && this._joined.length < this._blockSize) {
			callback();
			return;
		}
		let data = this._joined.joinPop();
		while(data.length) {
			//console.error(this._counter, data.length, this._mode);
			if(this._mode === MODE_CRC) {
				if(data.length >= 4) {
					this._crc = bufToCrc(data.slice(0, 4));
					this._mode = MODE_DATA;
					data = data.slice(4);
				} else {
					this._joined.push(data);
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
					this._joined.push(data);
					data = EMPTY_BUF;
				}
			}
		}
		callback();
	}

	_flush(callback) {
		// Last block might not be full-size, and now that we know we've reached
		// the end, we handle it here.
		if(!this._joined.length) {
			callback();
			return;
		}
		let buf = this._joined.joinPop();
		if(this._mode === MODE_CRC) {
			callback(new BadData(`Stream ended in the middle of a CRC32C: ${buf.toString('hex')}`));
			return;
		}
		// It should be smaller than the block size, else it would have been handled in _transform
		A(buf.length < this._blockSize, buf.length);
		const crc = sse4_crc32.calculate(buf);
		if(!this._checkCRC(callback, crc, this._crc)) {
			return;
		}
		this.push(buf);
		this._joined = null;
		callback();
	}
}


module.exports = {BadData, CRCWriter, CRCReader, crcToBuf};
