"use strong";
"use strict";

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const assert = require('assert');
const stream = require('stream');
const blake2 = require('blake2');
const Promise = require('bluebird');
const chopshop = require('chopshop');
const Combine = require('combine-streams');
const utils = require('../utils');

const CHUNK_SIZE = 100 * 1024;
// Chunk size must be a multiple of an AES block, for our convenience.
assert.strictEqual(CHUNK_SIZE % 128/8, 0);
const iv0 = new Buffer('00000000000000000000000000000000', 'hex');
assert.equal(iv0.length, 128/8);

const writeChunks = Promise.coroutine(function*(directory, key, p) {
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

class BadChunk extends Error {
	get name() {
		return this.constructor.name;
	}
}

/**
 * Returns a readable stream by decrypting and concatenating the chunks.
 */
function readChunks(directory, key, chunkDigests) {
	//T(directory, 'string', key, Buffer, chunkDigests, T.Array(Buffer));

	assert.equal(typeof directory, "string");
	assert(key instanceof Buffer, key);
	assert.equal(key.length, 128/8);
	assert(Array.isArray(chunkDigests), chunkDigests);

	const cipherStream = new Combine();
	const clearStream = crypto.createCipheriv('aes-128-ctr', key, iv0);
	// We don't return this Promise; we return the stream and
	// the coroutine does the work of writing to the stream.
	Promise.coroutine(function*() {
		for(const digest of chunkDigests) {
			assert(digest instanceof Buffer, digest);

			const chunkStream = fs.createReadStream(path.join(directory, digest.toString('hex')));

			const blake2b = blake2.createHash('blake2b');
			const passthrough = new stream.PassThrough();
			chunkStream.pipe(passthrough);
			passthrough.on('data', function(data) {
				blake2b.update(data);
			});

			cipherStream.append(passthrough);
			yield new Promise(function(resolve, reject) {
				chunkStream.once('end', function() {
					const readDigest = blake2b.digest().slice(0, 224/8);
					if(readDigest.equals(digest)) {
						resolve();
					} else {
						reject(new BadChunk(
							`BLAKE2b-224 of chunk should be\n` +
							`${digest.toString('hex')} but read data was \n` +
							`${readDigest.toString('hex')}`
						));
					}
				});
			});
		}
		cipherStream.append(null);
	})().catch(function(err) {
		clearStream.emit('error', err);
	});
	cipherStream.pipe(clearStream);
	return clearStream;
}

module.exports = {writeChunks, readChunks};
