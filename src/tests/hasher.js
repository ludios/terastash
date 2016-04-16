"use strong";
"use strict";

require('better-buffer-inspect');

const assert = require('assert');
const A = require('ayy');
const utils = require('../utils');
const hasher = require('../hasher');
const realistic_streamifier = require('../realistic_streamifier');
const Promise = require('bluebird');
const crypto = require('crypto');

describe('CRCWriter', function() {
	it("yields 0-byte stream for 0-byte input", Promise.coroutine(function*() {
		const inputBuf = new Buffer(0);
		const inputStream = realistic_streamifier.createReadStream(inputBuf);
		const writer = new hasher.CRCWriter(4096);
		utils.pipeWithErrors(inputStream, writer);
		const outputBuf = yield utils.readableToBuffer(writer);
		A.eq(outputBuf.length, 0);
	}));

	it("yields 5-byte stream for 1-byte input", Promise.coroutine(function*() {
		const inputBuf = new Buffer(1).fill(0);
		const inputStream = realistic_streamifier.createReadStream(inputBuf);
		const writer = new hasher.CRCWriter(4096);
		utils.pipeWithErrors(inputStream, writer);
		const outputBuf = yield utils.readableToBuffer(writer);
		A.eq(outputBuf.length, 5);
	}));
});

describe('CRCWriter+CRCReader', function() {
	it("works for all block sizes", Promise.coroutine(function*() {
		this.timeout(20000);
		for(const blockSize of [1, 2, 4, 10, 32, 64, 255, 256, 8 * 1024]) {
			// Need 8KB / 32MB test to catch lack-of-'this._buf = EMPTY_BUF;' bug
			const size =
				blockSize > 1024 ?
					32 * 1024 * 1024 :
					8 * 1024;
			const inputBuf = crypto.pseudoRandomBytes(size);

			//console.error(blockSize);
			let inputStream = realistic_streamifier.createReadStream(inputBuf);
			const writer = new hasher.CRCWriter(blockSize);
			utils.pipeWithErrors(inputStream, writer);
			const buf = yield utils.writableToBuffer(writer);
			A.eq(buf.length, size + (4 * Math.ceil(size / blockSize)));

			// Now, read it back
			const reader = new hasher.CRCReader(blockSize);
			inputStream = realistic_streamifier.createReadStream(buf);
			utils.pipeWithErrors(inputStream, reader);
			const outputBuf = yield utils.readableToBuffer(reader);
			assert.deepStrictEqual(outputBuf, inputBuf);
		}
	}));
});
