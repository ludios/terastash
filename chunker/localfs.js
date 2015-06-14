"use strong";
"use strict";

const A = require('ayy');
const T = require('notmytype');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const Promise = require('bluebird');
const chopshop = require('chopshop');
const Combine = require('combine-streams');
const utils = require('../utils');
const inspect = require('util').inspect;

const writeChunks = Promise.coroutine(function*(directory, cipherStream, chunkSize) {
	T(directory, T.string, cipherStream, T.shape({pipe: T.function}), chunkSize, T.number);

	// Chunk size must be a multiple of an AES block, for implementation convenience.
	A.eq(chunkSize % 128/8, 0);

	let totalSize = 0;
	const chunkInfo = [];

	let idx = 0;
	for(const chunkStream of chopshop.chunk(cipherStream, chunkSize)) {
		const tempFname = path.join(directory, '.temp-' + crypto.randomBytes(128/8).toString('hex'));
		const writeStream = fs.createWriteStream(tempFname);

		const hasher = utils.streamHasher(chunkStream, 'crc32c');
		hasher.stream.pipe(writeStream);

		yield new Promise(function(resolve) {
			writeStream.once('finish', Promise.coroutine(function*() {
				const size = (yield utils.statAsync(tempFname)).size;
				A.lte(size, chunkSize);
				totalSize += size;
				const crc32c = hasher.hash.digest();
				const file_id = utils.makeChunkFilename();
				chunkInfo.push({idx, file_id, size, crc32c});
				yield utils.renameAsync(
					tempFname,
					path.join(directory, file_id)
				);
				resolve();
			}));
		});
		idx += 1;
	}
	return [totalSize, chunkInfo];
});

class BadChunk extends Error {
	get name() {
		return this.constructor.name;
	}
}

/**
 * Returns a readable stream by decrypting and concatenating the chunks.
 */
function readChunks(directory, chunks) {
	T(directory, T.string, chunks, utils.ChunksType);

	const cipherStream = new Combine();
	// We don't return this Promise; we return the stream and
	// the coroutine does the work of writing to the stream.
	Promise.coroutine(function*() {
		for(const chunk of chunks) {
			const digest = chunk.crc32c;
			A.eq(digest.length, 32/8);

			const chunkStream = fs.createReadStream(path.join(directory, chunk.file_id));
			const hasher = utils.streamHasher(chunkStream, 'crc32c');
			cipherStream.append(hasher.stream);

			yield new Promise(function(resolve, reject) {
				chunkStream.once('end', function() {
					const readDigest = hasher.hash.digest();
					if(readDigest.equals(digest)) {
						resolve();
					} else {
						reject(new BadChunk(
							`CRC32C of chunk\n${inspect(chunk)} should be\n` +
							`${digest.toString('hex')} but read data was \n` +
							`${readDigest.toString('hex')}`
						));
					}
				});
			});
		}
		cipherStream.append(null);
	})().catch(function(err) {
		cipherStream.emit('error', err);
	});
	return cipherStream;
}

module.exports = {writeChunks, readChunks};
