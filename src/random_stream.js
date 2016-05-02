"use strict";

const crypto = require('crypto');
const path = require('path');
const utils = require('./utils');
const Readable = require('stream').Readable;

function makeKey() {
	if(Number(process.env.TERASTASH_INSECURE_AND_DETERMINISTIC)) {
		const keyCounter = new utils.PersistentCounter(
			path.join(process.env.TERASTASH_COUNTERS_DIR, 'random-stream-key-counter'));
		const buf = Buffer.alloc(128/8);
		buf.writeIntBE(keyCounter.getNext(), 0, 128/8);
		return buf;
	} else {
		return crypto.randomBytes(128/8);
	}
}

const ZERO_64KB = Buffer.alloc(64 * 1024);

class SecureRandomStream extends Readable {
	constructor(wantLength) {
		utils.assertSafeNonNegativeInteger(wantLength);
		super();
		this._bytesWritten = 0;
		this._wantLength = wantLength;
		this._cipher = crypto.createCipheriv('aes-128-ctr', makeKey(), makeKey());
	}

	_read() {
		let randomBuf = this._cipher.update(ZERO_64KB);
		if(this._bytesWritten + randomBuf.length > this._wantLength) {
			randomBuf = randomBuf.slice(0, this._wantLength - this._bytesWritten);
			this.push(randomBuf);
			this.push(null);
		} else {
			this.push(randomBuf);
		}
		this._bytesWritten += randomBuf.length;
	}
}

module.exports = {SecureRandomStream};
