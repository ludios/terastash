"use strong";
"use strict";

const T = require('notmytype');
const A = require('ayy');
const utils = require('./utils');
const inspect = require('util').inspect;
const commaify = utils.commaify;
const Transform = require('stream').Transform;
const JoinedBuffers = require('./utils').JoinedBuffers;

class BadData extends Error {
	get name() {
		return this.constructor.name;
	}
}

const MODE_DATA = Symbol("MODE_DATA");
const MODE_LEN = Symbol("MODE_LEN");
const EMPTY_BUF = new Buffer(0);

class Int32BufferDecoder extends Transform {
	constructor(endianness, maxLength) {
		T(endianness, T.string, maxLength, T.number);
		utils.assertSafeNonNegativeInteger(maxLength);

		// Even though we emit buffers, we obviously don't want them joined back
		// together by node streams.
		super({readableObjectMode: true});

		let readInt;
		if(endianness === "LE") {
			readInt = (b) => b.readUInt32LE(0);
		} else if(endianness === "BE") {
			readInt = (b) => b.readUInt32BE(0);
		} else {
			throw new Error(`endianness must be "LE" or "BE", was ${inspect(endianness)}`);
		}
		this._readInt = readInt;
		this._maxLength = maxLength;
		// Use JoinedBuffers because doing Buffer.concat on every _transform
		// call would exhibit O(N^2) behavior for long frames.
		this._joined = new JoinedBuffers();
		this._mode = MODE_LEN;
		this._currentLength = null;
	}

	_transform(newData, encoding, callback) {
		T(newData, Buffer);
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
					this._currentLength = this._readInt(data.slice(0, 4));
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

module.exports = {Int32BufferDecoder, BadData};
