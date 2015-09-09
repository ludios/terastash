"use strong";
"use strict";

require('better-buffer-inspect');

const assert = require('assert');
const A = require('ayy');
const utils = require('../utils');
const fs = require('fs');
const os = require('os');
const hasher = require('../hasher');
const realistic_streamifier = require('../realistic_streamifier');
const Promise = require('bluebird');
const crypto = require('crypto');

describe('CRCWriter+CRCReader', function() {
	it("works for all block sizes", Promise.coroutine(function*() {
		this.timeout(60000);
		for(const blockSize of [1, 2, 4, 10, 32, 64, 255, 256, 8 * 1024]) {
			// Need 8KB / 32MB test to catch lack-of-'this._buf = EMPTY_BUF;' bug
			const size =
				blockSize > 1024 ?
					32 * 1024 * 1024 :
					8 * 1024;
			const inputBuf = crypto.pseudoRandomBytes(size);

			//console.error(blockSize);
			let inputStream = realistic_streamifier.createReadStream(inputBuf);
			const tempfname = `${os.tmpdir()}/terastash_tests_hasher_crc`;
			const writer = new hasher.CRCWriter(blockSize);
			utils.pipeWithErrors(inputStream, writer);
			const outputStream = fs.createWriteStream(tempfname);
			utils.pipeWithErrors(writer, outputStream);
			yield new Promise(function(resolve) {
				outputStream.once('finish', resolve);
			});
			A.eq(fs.statSync(tempfname).size, size + (4 * Math.ceil(size / blockSize)));

			// Now, read it back
			const reader = new hasher.CRCReader(blockSize);
			inputStream = fs.createReadStream(tempfname);
			utils.pipeWithErrors(inputStream, reader);
			const outputBuf = yield utils.readableToBuffer(reader);
			assert.deepStrictEqual(outputBuf, inputBuf);
		}
	}));
});
