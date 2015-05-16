"use strict";

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const assert = require('assert');
const stream = require('stream');
const blake2 = require('blake2');
const co = require('co');
const chopshop = require('chopshop');
const utils = require('../utils');

const CHUNK_SIZE = 100 * 1024;

function writeChunks(directory, key, p) {
	const expectedTotalSize = fs.statSync(p).size;
	const inputStream = fs.createReadStream(p);
	const iv0 = new Buffer('00000000000000000000000000000000', 'hex');
	assert.equal(iv0.length, 128/8);
	assert.equal(key.length, 128/8);
	const cipherStream = crypto.createCipheriv('aes-128-ctr', key, iv0);
	inputStream.pipe(cipherStream);
	let totalSize = 0;
	return co(function*() {
		for(const chunkStream of chopshop.chunk(cipherStream, CHUNK_SIZE)) {
			const tempFname = path.join(directory, 'temp-' + Math.random());
			const writeStream = fs.createWriteStream(tempFname);
			const blake2b = blake2.createHash('blake2b');
			const passthrough = new stream.PassThrough();
			chunkStream.pipe(passthrough);
			passthrough.on('data', function(data) {
				blake2b.update(data);
			});
			passthrough.pipe(writeStream);
			yield new Promise(function(resolve) {
				writeStream.once('finish', function() {
					const size = fs.statSync(tempFname).size;
					assert(size <= CHUNK_SIZE, size);
					totalSize += size;
					const hexDigest = blake2b.digest().slice(0, 224/8).toString('hex');
					fs.renameSync(
						tempFname,
						path.join(directory, hexDigest)
					);
					resolve();
				});
			});
		}
		assert.equal(totalSize, expectedTotalSize,
			`Wrote \n${utils.numberWithCommas(totalSize)} bytes to chunks instead of the expected\n` +
			`${utils.numberWithCommas(expectedTotalSize)} bytes; did file change during reading?`);
	});
}

module.exports = {writeChunks};
