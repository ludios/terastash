"use strong";
"use strict";

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const assert = require('assert');
const stream = require('stream');
const blake2 = require('blake2');
const co = require('co');
const Promise = require('bluebird');
const chopshop = require('chopshop');
const Combine = require('combine-streams');
const utils = require('../utils');

const CHUNK_SIZE = 100 * 1024;
const iv0 = new Buffer('00000000000000000000000000000000', 'hex');
assert.equal(iv0.length, 128/8);

function writeChunks(directory, key, p) {
	assert.equal(typeof directory, "string");
	assert.equal(typeof p, "string");
	assert(key instanceof Buffer, key);
	assert.equal(key.length, 128/8);

	const expectedTotalSize = fs.statSync(p).size;
	const inputStream = fs.createReadStream(p);
	const cipherStream = crypto.createCipheriv('aes-128-ctr', key, iv0);
	inputStream.pipe(cipherStream);
	let totalSize = 0;
	const chunkDigests = [];
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
					const digest = blake2b.digest().slice(0, 224/8);
					chunkDigests.push(digest);
					fs.renameSync(
						tempFname,
						path.join(directory, digest.toString('hex'))
					);
					resolve();
				});
			});
		}
		assert.equal(totalSize, expectedTotalSize,
			`Wrote \n${utils.numberWithCommas(totalSize)} bytes to chunks instead of the expected\n` +
			`${utils.numberWithCommas(expectedTotalSize)} bytes; did file change during reading?`);
		return chunkDigests;
	});
}

/**
 * Returns a readable stream by decrypting and concatenating the chunks.
 */
function readChunks(directory, key, chunkDigests) {
	assert.equal(typeof directory, "string");
	assert(key instanceof Buffer, key);
	assert.equal(key.length, 128/8);
	assert(Array.isArray(chunkDigests), chunkDigests);

	// TODO: check hashes
	const cipherStream = new Combine();
	co(function*() {
		for(const digest of chunkDigests) {
			const chunkStream = fs.createReadStream(path.join(directory, digest.toString('hex')));
			cipherStream.append(chunkStream);
			yield new Promise(function(resolve) {
				chunkStream.once('end', function() {
					resolve();
				});
			});
		}
		cipherStream.append(null);
	}).catch(function(err) {
		// TODO: emit the error through the streams instead?
		console.log(err.stack);
	});
	const clearStream = crypto.createCipheriv('aes-128-ctr', key, iv0);
	cipherStream.pipe(clearStream);
	return clearStream;
}

module.exports = {writeChunks, readChunks};
