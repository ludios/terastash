"use strong";
"use strict";

const utils = require('./utils');
const Transform = require('stream').Transform;

class Padder extends Transform {
	constructor(padToLength) {
		utils.assertSafeNonNegativeInteger(padToLength);
		super();
		this.bytesRead = 0;
		this._padToLength = padToLength;
	}

	_transform(data, encoding, callback) {
		this.bytesRead += data.length;
		callback(null, data);
	}

	_flush(callback) {
		if(this._padToLength > this.bytesRead) {
			this.push(new Buffer(this._padToLength - this.bytesRead).fill(0));
		}
		callback();
	}
}

class Unpadder extends Transform {
	constructor(unpadToLength) {
		utils.assertSafeNonNegativeInteger(unpadToLength);
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
			callback(null, data);
		} else {
			callback(null, data.slice(0, data.length - (this.bytesRead - this._unpadToLength)));
		}
	}
}

module.exports = {Padder, Unpadder};
