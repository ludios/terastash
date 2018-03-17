"use strict";

require('better-buffer-inspect');

const assert                = require('assert');
const A                     = require('ayy');
const utils                 = require('../utils');
const hasher                = require('../hasher');
const realistic_streamifier = require('../realistic_streamifier');
const Promise               = require('bluebird');
const crypto                = require('crypto');

describe('CRCWriter', function() {
	it("yields 0-byte stream for 0-byte input", async function() {
		const inputBuf = Buffer.alloc(0);
		const inputStream = realistic_streamifier.createReadStream(inputBuf);
		const writer = new hasher.CRCWriter(4096);
		utils.pipeWithErrors(inputStream, writer);
		const outputBuf = await utils.readableToBuffer(writer);
		A.eq(outputBuf.length, 0);
	});

	it("yields 5-byte stream for 1-byte input", async function() {
		const inputBuf = Buffer.alloc(1);
		const inputStream = realistic_streamifier.createReadStream(inputBuf);
		const writer = new hasher.CRCWriter(4096);
		utils.pipeWithErrors(inputStream, writer);
		const outputBuf = await utils.readableToBuffer(writer);
		A.eq(outputBuf.length, 5);
	});
});

describe('CRCWriter+CRCReader', function() {
	it("works for all block sizes", async function() {
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
			const buf = await utils.writableToBuffer(writer);
			A.eq(buf.length, size + (4 * Math.ceil(size / blockSize)));

			// Now, read it back
			const reader = new hasher.CRCReader(blockSize);
			inputStream = realistic_streamifier.createReadStream(buf);
			utils.pipeWithErrors(inputStream, reader);
			const outputBuf = await utils.readableToBuffer(reader);
			assert.deepStrictEqual(outputBuf, inputBuf);
		}
	});
});
