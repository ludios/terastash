"use strict";

const A = require('ayy');
const T = require('notmytype');
const fs = require('../fs-promisified');
const path = require('path');
const crypto = require('crypto');
const Promise = require('bluebird');
const Combine = require('combine-streams');
const utils = require('../utils');
const OutputContextType = utils.OutputContextType;
const inspect = require('util').inspect;
const chalk = require('chalk');

const writeChunks = Promise.coroutine(function* writeChunks$coro(outCtx, directory, getChunkStream) {
	T(outCtx, OutputContextType, directory, T.string, getChunkStream, T.function);

	let totalSize = 0;
	let idx = 0;
	const chunkInfo = [];

	while(true) {
		const chunkStream = yield getChunkStream(false);
		if(chunkStream === null) {
			break;
		}
		const tempFname = path.join(directory, '.temp-' + crypto.randomBytes(128/8).toString('hex'));
		const writeStream = fs.createWriteStream(tempFname);

		const hasher = utils.streamHasher(chunkStream, 'crc32c');
		utils.pipeWithErrors(hasher.stream, writeStream);

		yield new Promise(function writeChunks$Promise(resolve) {
			writeStream.once('finish', Promise.coroutine(function*() {
				const size = (yield fs.statAsync(tempFname)).size;
				totalSize += size;
				const crc32c = hasher.hash.digest();
				const file_id = utils.makeChunkFilename();
				chunkInfo.push({idx, file_id, size, crc32c});
				yield fs.renameAsync(
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
function readChunks(directory, chunks, ranges, checkWholeChunkCRC32C) {
	T(directory, T.string, chunks, utils.ChunksType, ranges, utils.RangesType, checkWholeChunkCRC32C, T.boolean);
	A.eq(chunks.length, ranges.length);

	const cipherStream = new Combine();
	// We don't return this Promise; we return the stream and
	// the coroutine does the work of writing to the stream.
	Promise.coroutine(function* readChunks$coro() {
		for(const [chunk, range] of utils.zip(chunks, ranges)) {
			const digest = chunk.crc32c;
			A.eq(digest.length, 32/8);

			const chunkStream = fs.createReadStream(
				path.join(directory, chunk.file_id),
				{start: range[0], end: range[1] - 1}); // end is inclusive
			let hasher;
			if(checkWholeChunkCRC32C) {
				hasher = utils.streamHasher(chunkStream, 'crc32c');
				cipherStream.append(hasher.stream);
			} else {
				cipherStream.append(chunkStream);
			}

			yield new Promise(function readChunks$Promise(resolve, reject) {
				(hasher ? hasher.stream : chunkStream).once('end', function() {
					if(!checkWholeChunkCRC32C) {
						resolve();
						return;
					}
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

/**
 * Deletes chunks
 */
const deleteChunks = Promise.coroutine(function* deleteChunks$coro(directory, chunks) {
	T(directory, T.string, chunks, utils.ChunksType);
	for(const chunk of chunks) {
		try {
			yield fs.unlinkAsync(path.join(directory, chunk.file_id));
		} catch(err) {
			console.error(chalk.red(
				`Failed to delete chunk with file_id=${inspect(chunk.file_id)}` +
				` (chunk #${chunk.idx} for file)`));
			console.error(chalk.red(err.stack));
		}
	}
});

module.exports = {writeChunks, readChunks, deleteChunks};
