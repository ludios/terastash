"use strong";
"use strict";

require('better-buffer-inspect');

const assert = require('assert');
const A = require('ayy');
const utils = require('../utils');
const fs = require('fs');
const os = require('os');
const hasher = require('../hasher');
const streamifier = require('streamifier');
const Promise = require('bluebird');
const crypto = require('crypto');

describe('CRCWriter+CRCReader', function() {
	it("works for any block size", Promise.coroutine(function*() {
		this.timeout(20000);
		const size = 8 * 1024;
		const inputBuf = crypto.pseudoRandomBytes(size);
		for(const blockSize of [1, 2, 4, 10, 32, 64, 255, 256]) {
			//console.error(blockSize);
			let inputStream = streamifier.createReadStream(inputBuf);
			const tempfname = `${os.tmpdir()}/terastash_tests_hasher_crc`;
			const writer = new hasher.CRCWriter(blockSize);
			utils.pipeWithErrors(inputStream, writer);
			const outputStream = fs.createWriteStream(tempfname);
			utils.pipeWithErrors(writer, outputStream);
			yield new Promise(function(resolve, reject) {
				outputStream.on('finish', resolve);
			});
			A.eq(fs.statSync(tempfname).size, size + (4 * Math.ceil(size / blockSize)));

			// Now, read it back
			const reader = new hasher.CRCReader(blockSize);
			inputStream = fs.createReadStream(tempfname);
			utils.pipeWithErrors(inputStream, reader);
			const outputBuf = yield utils.streamToBuffer(reader);
			assert.deepStrictEqual(outputBuf, inputBuf);
		}
	}));
});
