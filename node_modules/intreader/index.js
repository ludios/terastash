"use strong";
"use strict";

const assert = require('assert');
const inspect = require('util').inspect;
const Transform = require('stream').Transform;

function commaify(stringOrNum) {
	// http://stackoverflow.com/questions/2901102/
	return String(stringOrNum).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

class BadData extends Error {
	get name() {
		return this.constructor.name;
	}
}

const EMPTY_BUF = new Buffer(0);

/**
 * An object that holds multiple Buffers and knows the total
 * length, allowing you to delay the .concat() until you need
 * the whole thing.
 */
class JoinedBuffers {
	constructor() {
		this._bufs = [];
		this.length = 0;
	}

	push(buf) {
		this.length += buf.length;
		this._bufs.push(buf);
	}

	joinPop() {
		if(!this._bufs.length) {
			return EMPTY_BUF;
		}
		const bufs = this._bufs;
		this._bufs = [];
		this.length = 0;
		if(bufs.length === 1) {
			return bufs[0];
		} else {
			return Buffer.concat(bufs);
		}
	}
}

const MODE_DATA = Symbol("MODE_DATA");
const MODE_LEN = Symbol("MODE_LEN");

class Int32Reader extends Transform {
	/**
	 * endianness - the endianness of the length int, either "LE" or "BE"
	 * maxLength - the maximum allowed length of a frame, not including the 4 bytes for the int itself.
	 * lengthIncludesInt - use true if the length includes the size of the length int itself, else false.
	 */
	constructor(endianness, maxLength, lengthIncludesInt) {
		// Even though we emit buffers, we obviously don't want them joined back
		// together by node streams.
		super({readableObjectMode: true});

		let readInt;
		if(endianness === "LE") {
			readInt = function(b) {
				return b.readUInt32LE(0);
			};
		} else if(endianness === "BE") {
			readInt = function(b) {
				return b.readUInt32BE(0);
			};
		} else {
			throw new Error(`endianness must be "LE" or "BE", was ${inspect(endianness)}`);
		}

		assert(maxLength >= 0 && Number.isSafeInteger(maxLength),
			`maxLength must be a safe integer >= 0; was ${inspect(maxLength)}`);

		this._readInt = readInt;
		this._maxLength = maxLength;
		this._lengthIncludesInt = !!lengthIncludesInt;
		// Use JoinedBuffers because doing Buffer.concat on every _transform
		// call would exhibit O(N^2) behavior for long frames.
		this._joined = new JoinedBuffers();
		this._mode = MODE_LEN;
		this._currentLength = null;
	}

	_transform(newData, encoding, callback) {
		this._joined.push(newData);
		// Don't bother processing anything if we don't have enough to decode
		// a length or the data.
		if(this._mode === MODE_LEN && this._joined.length < 4 ||
		this._mode === MODE_DATA && this._joined.length < this._currentLength) {
			callback();
			return;
		}
		let data = this._joined.joinPop();
		while(data.length) {
			//console.error(data.length, this._mode);
			if(this._mode === MODE_LEN) {
				if(data.length >= 4) {
					this._currentLength = this._readInt(data);
					if(this._lengthIncludesInt) {
						if(this._currentLength < 4) {
							callback(new BadData(
								`Frame size includes the length integer itself, so it cannot ` +
								`be less than 4, but was ${commaify(this._currentLength)}`));
							return;
						}
						this._currentLength -= 4;
					}
					if(this._currentLength > this._maxLength) {
						callback(new BadData(
							`Frame size ${commaify(this._currentLength)} ` +
							`exceeds max length ${commaify(this._maxLength)}`));
						return;
					}
					this._mode = MODE_DATA;
					data = data.slice(4);
				} else {
					this._joined.push(data);
					data = EMPTY_BUF;
				}
			} else if(this._mode === MODE_DATA) {
				if(data.length >= this._currentLength) {
					this.push(data.slice(0, this._currentLength));
					this._mode = MODE_LEN;
					data = data.slice(this._currentLength);
				} else {
					this._joined.push(data);
					data = EMPTY_BUF;
				}
			}
		}
		callback();
	}

	_flush(callback) {
		// All data should be handled before the stream ends.
		if(!this._joined.length) {
			callback();
			return;
		}
		let buf = this._joined.joinPop();
		if(this._mode === MODE_LEN) {
			callback(new BadData(`Stream ended in the middle of a frame length: ${buf.toString('hex')}`));
			return;
		} else if(this._mode === MODE_DATA) {
			callback(new BadData(`Stream ended in the middle of frame data: ${buf.toString('hex')}`));
			return;
		}
		this._joined = null;
		callback();
	}
}

module.exports = {Int32Reader, BadData};
