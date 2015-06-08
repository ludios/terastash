"use strong";
"use strict";

const A = require('ayy');
const T = require('notmytype');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const stream = require('stream');
const Promise = require('bluebird');
const chopshop = require('chopshop');
const Combine = require('combine-streams');
const utils = require('../utils');
const blake2 = utils.requireBlake2();

const CHUNK_SIZE = 100 * 1024;
// Chunk size must be a multiple of an AES block, for our convenience.
A.eq(CHUNK_SIZE % 128/8, 0);
const iv0 = new Buffer('00000000000000000000000000000000', 'hex');
A.eq(iv0.length, 128/8);

const writeChunks = Promise.coroutine(function*(directory, key, p) {
	T(directory, T.string, key, Buffer, p, T.string);
	A.eq(key.length, 128/8);

	const expectedTotalSize = (yield utils.statAsync(p)).size;
	const inputStream = fs.createReadStream(p);
	const cipherStream = crypto.createCipheriv('aes-128-ctr', key, iv0);
	inputStream.pipe(cipherStream);
	let totalSize = 0;
	const chunkInfo = [];

	let idx = 0;
	for(const chunkStream of chopshop.chunk(cipherStream, CHUNK_SIZE)) {
		const tempFname = path.join(directory, 'temp-' + Math.random());
		const writeStream = fs.createWriteStream(tempFname);

		const _ = utils.streamHasher(chunkStream, 'blake2b');
		const passthrough = _[0];
		const blake2b = _[1];
		passthrough.pipe(writeStream);

		yield new Promise(function(resolve) {
			writeStream.once('finish', Promise.coroutine(function*() {
				const size = (yield utils.statAsync(tempFname)).size;
				A.lte(size, CHUNK_SIZE);
				totalSize += size;
				const digest = blake2b.digest().slice(0, 224/8);
				chunkInfo.push({idx, file_id: digest.toString('hex'), size});
				yield utils.renameAsync(
					tempFname,
					path.join(directory, digest.toString('hex'))
				);
				resolve();
			}));
		});
		idx += 1;
	}
	A.eq(totalSize, expectedTotalSize,
		`Wrote \n${utils.numberWithCommas(totalSize)} bytes to chunks instead of the expected\n` +
		`${utils.numberWithCommas(expectedTotalSize)} bytes; did file change during reading?`);
	return chunkInfo;
});

class BadChunk extends Error {
	get name() {
		return this.constructor.name;
	}
}

/**
 * Returns a readable stream by decrypting and concatenating the chunks.
 */
function readChunks(directory, key, chunks) {
	T(
		directory, T.string,
		key, Buffer,
		chunks, T.list(
			T.shape({
				"idx": T.number,
				"file_id": T.string,
				"size": T.object /* bigint */
			})
		)
	);
	A.eq(key.length, 128/8);

	const cipherStream = new Combine();
	const clearStream = crypto.createCipheriv('aes-128-ctr', key, iv0);
	// We don't return this Promise; we return the stream and
	// the coroutine does the work of writing to the stream.
	Promise.coroutine(function*() {
		for(const chunk of chunks) {
			const chunkStream = fs.createReadStream(path.join(directory, chunk.file_id));

			// For localfs, the filename is the digest
			const digest = Buffer(chunk.file_id, "hex");
			A.eq(digest.length, 224/8);

			const _ = utils.streamHasher(chunkStream, 'blake2b');
			const passthrough = _[0];
			const blake2b = _[1];
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
