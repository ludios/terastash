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

class RightTruncate extends Transform {
	constructor(desiredLength) {
		utils.assertSafeNonNegativeInteger(desiredLength);
		super();
		this.bytesRead = 0;
		this._desiredLength = desiredLength;
	}

	_transform(data, encoding, callback) {
		// If we already read past the length we want, drop the rest of the data.
		if(this.bytesRead >= this._desiredLength) {
			callback();
			return;
		}
		this.bytesRead += data.length;
		if(this.bytesRead <= this._desiredLength) {
			callback(null, data);
		} else {
			callback(null, data.slice(0, data.length - (this.bytesRead - this._desiredLength)));
		}
	}
}

class LeftTruncate extends Transform {
	constructor(skipBytes) {
		utils.assertSafeNonNegativeInteger(skipBytes);
		super();
		this.bytesRead = 0;
		this._skipBytes = skipBytes;
	}

	_transform(data, encoding, callback) {
		if(this.bytesRead >= this._skipBytes) {
			this.push(data);
		} else {
			this.push(data.slice(this._skipBytes - this.bytesRead));
		}
		this.bytesRead += data.length;
		callback();
	}
}

module.exports = {Padder, RightTruncate, LeftTruncate};
