"use strong";
"use strict";

const T = require('notmytype');
const A = require('ayy');
const Transform = require('stream').Transform;

class Padder extends Transform {
	constructor(padToLength) {
		T(padToLength, T.number);
		A(Number.isInteger(padToLength), padToLength);
		super();
		this._bytesRead = 0;
		this._padToLength = padToLength;
	}

	_transform(data, encoding, callback) {
		this._bytesRead += data.length;
		callback(null, data);
	}

	_flush(callback) {
		if(this._padToLength <= this._bytesRead) {
			callback();
		} else {
			callback(null, new Buffer(this._padToLength - this._bytesRead).fill(0));
		}
	}
}

class Unpadder extends Transform {
	constructor(unpadToLength) {
		T(unpadToLength, T.number);
		A(Number.isInteger(unpadToLength), unpadToLength);
		super();
		this._bytesRead = 0;
		this._unpadToLength = unpadToLength;
	}

	_transform(data, encoding, callback) {
		// If we already read past  the length we want, drop the rest of the data.
		if(this._bytesRead >= this._unpadToLength) {
			callback();
		}
		this._bytesRead += data.length;
		if(this._bytesRead <= this._unpadToLength) {
			callback(null, data);
		} else {
			callback(null, data.slice(0, data.length - (this._bytesRead - this._unpadToLength)));
		}
	}
}
