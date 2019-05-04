"use strict";

require('better-buffer-inspect');

const assert                = require('assert');
const A                     = require('ayy');
const utils                 = require('../utils');
const gcmer                 = require('../gcmer');
const realistic_streamifier = require('../realistic_streamifier');
const crypto                = require('crypto');

describe('gcmer.blockNumberToIv()', function() {
	it('returns correct results', function() {
		assert.deepStrictEqual(
			gcmer.blockNumberToIv(0),
			Buffer.from('000000000000000000000000', 'hex')
		);
		assert.deepStrictEqual(
			gcmer.blockNumberToIv(1),
			Buffer.from('000000000000000000000001', 'hex')
		);
		assert.deepStrictEqual(
			gcmer.blockNumberToIv(100),
			Buffer.from('000000000000000000000064', 'hex')
		);
		assert.deepStrictEqual(
			gcmer.blockNumberToIv(Math.pow(2, 53) - 1),
			Buffer.from('00000000001fffffffffffff', 'hex')
		);
	});
});

describe('gcmer.selfTest()', function() {
	it('works', function() {
		gcmer.selfTest();
	});
});

const KEY = crypto.pseudoRandomBytes(16);

describe('GCMWriter', function() {
	it("yields 0-byte stream for 0-byte input", async function() {
		const inputBuf = Buffer.alloc(0);
		const inputStream = realistic_streamifier.createReadStream(inputBuf);
		const writer = new gcmer.GCMWriter(4096, KEY, 0);
		utils.pipeWithErrors(inputStream, writer);
		const outputBuf = await utils.readableToBuffer(writer);
		A.eq(outputBuf.length, 0);
	});

	it("yields 17-byte stream for 1-byte input", async function() {
		const inputBuf = Buffer.alloc(1);
		const inputStream = realistic_streamifier.createReadStream(inputBuf);
		const writer = new gcmer.GCMWriter(4096, KEY, 0);
		utils.pipeWithErrors(inputStream, writer);
		const outputBuf = await utils.readableToBuffer(writer);
		A.eq(outputBuf.length, 17);
	});
});

describe('GCMWriter+GCMReader', function() {
	it("works for all block sizes", async function() {
		this.timeout(30000);
		for (const blockSize of [1, 2, 4, 10, 32, 64, 255, 256, 8 * 1024]) {
			// Need 8KB / 32MB test to catch lack-of-'this._buf = EMPTY_BUF;' bug
			const size =
				blockSize > 1024 ?
					32 * 1024 * 1024 :
					8 * 1024;
			const inputBuf = crypto.pseudoRandomBytes(size);

			//console.error(blockSize);
			let inputStream = realistic_streamifier.createReadStream(inputBuf);
			const writer = new gcmer.GCMWriter(blockSize, KEY, 0);
			utils.pipeWithErrors(inputStream, writer);
			const buf = await utils.writableToBuffer(writer);
			A.eq(buf.length, size + (16 * Math.ceil(size / blockSize)));

			// Now, read it back
			const reader = new gcmer.GCMReader(blockSize, KEY, 0);
			inputStream = realistic_streamifier.createReadStream(buf);
			utils.pipeWithErrors(inputStream, reader);
			const outputBuf = await utils.readableToBuffer(reader);
			assert.deepStrictEqual(outputBuf, inputBuf);
		}
	});
});
