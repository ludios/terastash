"use strict";

const crypto        = require('crypto');
const A             = require('ayy');
const T             = require('notmytype');
const utils         = require('./utils');
const aes           = require('./aes');
const commaify      = utils.commaify;
const JoinedBuffers = utils.JoinedBuffers;
const Transform     = require('stream').Transform;

const IV_SIZE = 12;

// terastash block, not AES block
function blockNumberToIv(blockNum) {
	utils.assertSafeNonNegativeInteger(blockNum);
	const buf = Buffer.from(aes.strictZeroPad(blockNum.toString(16), IV_SIZE * 2), 'hex');
	A(buf.length, IV_SIZE);
	return buf;
}

/**
 * Make sure that OpenSSL's AES-128-GCM cipher works as expected, including
 * our use of the IV as the counter.
 */
function selfTest() {
	const key = Buffer.from('12300000000000045600000000000789', 'hex');
	const iv1 = blockNumberToIv(1);

	// Test for exact ciphertext
	const cipher    = crypto.createCipheriv('aes-128-gcm', key, iv1);
	const text      = 'Hello, world. This is a test string spanning multiple AES blocks.';
	const encrypted = cipher.update(Buffer.from(text));
	cipher.final();
	const tag = cipher.getAuthTag();
	A.eq(
		encrypted.toString('hex'),
		'ad7c82cce770a643f140a3c1d8a9da65ad91f7175d6326dc3c7ddc743cd82fa5f' +
		'2b5da363e1ec0834ae94cd333f8acb64b7a154b58cb9206bed6f78023dfba6068'
	);

	// Test that encryption->decryption round-trips
	const decipher = crypto.createDecipheriv('aes-128-gcm', key, iv1);
	decipher.setAuthTag(tag);
	const decrypted = decipher.update(encrypted);
	A.eq(decrypted.toString('utf-8'), text);
}

class GCMWriter extends Transform {
	constructor(blockSize, key, initialBlockNum) {
		T(blockSize, T.number, key, Buffer, initialBlockNum, T.number);
		utils.assertSafeNonNegativeInteger(blockSize);
		utils.assertSafeNonNegativeInteger(initialBlockNum);
		A.gt(blockSize, 0);
		A.eq(key.length, 128 / 8);
		super();
		this._blockSize = blockSize;
		this._key = key;
		this._blockNum = initialBlockNum;
		this._joined = new JoinedBuffers();
	}

	_pushTagAndEncryptedBuf(buf) {
		//console.error("GCMWriter", {iv: this._blockNum});
		const cipher = crypto.createCipheriv('aes-128-gcm', this._key, blockNumberToIv(this._blockNum));
		const oldBlockNum = this._blockNum;
		this._blockNum += 1;
		// Because we're dealing with JS doubles, make sure that the IV is no longer the same.
		A.gt(this._blockNum, oldBlockNum);
		const encryptedBuf = cipher.update(buf);
		const ret = cipher.final();
		A.eq(ret.length, 0);
		const tag = cipher.getAuthTag();
		A.eq(tag.length, 128 / 8);
		this.push(tag);
		this.push(encryptedBuf);
	}

	_transform(data, encoding, callback) {
		this._joined.push(data);
		// Can write out at least one new block?
		if(this._joined.length >= this._blockSize) {
			const [splitBufs, remainder] = utils.splitBuffer(this._joined.joinPop(), this._blockSize);
			this._joined.push(remainder);

			for (const buf of splitBufs) {
				this._pushTagAndEncryptedBuf(buf);
			}
		}
		callback();
	}

	_flush(callback) {
		// Need to write out the last block, even if it's under-sized
		if(this._joined.length > 0) {
			const buf = this._joined.joinPop();
			this._pushTagAndEncryptedBuf(buf);
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
const MODE_TAG = Symbol("MODE_TAG");
const EMPTY_BUF = Buffer.alloc(0);

class GCMReader extends Transform {
	constructor(blockSize, key, initialBlockNum) {
		T(blockSize, T.number, key, Buffer, initialBlockNum, T.number);
		utils.assertSafeNonNegativeInteger(blockSize);
		utils.assertSafeNonNegativeInteger(initialBlockNum);
		A.gt(blockSize, 0);
		A.eq(key.length, 128 / 8);
		super();
		this._blockSize = blockSize;
		this._key = key;
		this._blockNum = initialBlockNum;
		this._joined = new JoinedBuffers();
		this._tag = null;
		this._mode = MODE_TAG;
	}

	_decrypt(callback, buf) {
		const decipher = crypto.createDecipheriv('aes-128-gcm', this._key, blockNumberToIv(this._blockNum));
		decipher.setAuthTag(this._tag);
		const decryptedBuf = decipher.update(buf);
		try {
			const ret = decipher.final();
			A.eq(ret.length, 0);
		} catch(e) {
			callback(new BadData(
				`Authenticated decryption of block ${commaify(this._blockNum)} failed:\n` +
				`${e}`)
			);
			return false;
		}
		const oldBlockNum = this._blockNum;
		this._blockNum += 1;
		// Because we're dealing with JS doubles, make sure that the IV is no longer the same.
		A.gt(this._blockNum, oldBlockNum);
		this.push(decryptedBuf);
		return true;
	}

	_transform(newData, encoding, callback) {
		this._joined.push(newData);
		// Don't bother processing anything if we don't have enough to decode
		// a tag or a block.
		if(this._mode === MODE_TAG && this._joined.length < 16 ||
		this._mode === MODE_DATA && this._joined.length < this._blockSize) {
			callback();
			return;
		}
		let data = this._joined.joinPop();
		while (data.length) {
			//console.error(this._counter, data.length, this._mode);
			if(this._mode === MODE_TAG) {
				if(data.length >= 16) {
					this._tag = data.slice(0, 16);
					this._mode = MODE_DATA;
					data = data.slice(16);
				} else {
					this._joined.push(data);
					data = EMPTY_BUF;
				}
			} else if(this._mode === MODE_DATA) {
				if(data.length >= this._blockSize) {
					const block = data.slice(0, this._blockSize);
					if(!this._decrypt(callback, block)) {
						return;
					}
					this._mode = MODE_TAG;
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
		if(this._mode === MODE_TAG) {
			callback(new BadData(`Stream ended in the middle of a tag: ${buf.toString('hex')}`));
			return;
		}
		// It should be smaller than the block size, else it would have been handled in _transform
		A(buf.length < this._blockSize, buf.length);
		if(!this._decrypt(callback, buf)) {
			return;
		}
		this._joined = null;
		callback();
	}
}


module.exports = {blockNumberToIv, BadData, GCMWriter, GCMReader, selfTest};
