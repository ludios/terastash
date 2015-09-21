"use strong";
"use strict";

require('better-buffer-inspect');

const assert = require('assert');
const A = require('ayy');
const T = require('notmytype');
const utils = require('../utils');
const frame_reader = require('../frame_reader');
const realistic_streamifier = require('../realistic_streamifier');
const crypto = require('crypto');
const Promise = require('bluebird');

function makeFrame(buf) {
	const len = new Buffer(4);
	len.writeUInt32LE(buf.length, 0);
	return Buffer.concat([len, buf]);
}

function readableToArray(stream) {
	T(stream, utils.StreamType);
	return new Promise(function readableToBuffer$Promise(resolve, reject) {
		const objs = [];
		stream.on('data', function(data) {
			objs.push(data);
		});
		stream.once('end', function() {
			resolve(objs);
		});
		stream.once('error', function(err) {
			reject(err);
		});
		stream.resume();
	});
}

describe('Int32BufferDecoder', function() {
	it("yields 0 frames for 0-byte input", Promise.coroutine(function*() {
		const inputBuf = new Buffer(0);
		const inputStream = realistic_streamifier.createReadStream(inputBuf);
		const reader = new frame_reader.Int32BufferDecoder("LE", 512 * 1024);
		utils.pipeWithErrors(inputStream, reader);
		const output = yield readableToArray(reader);
		A.eq(output.length, 0);
	}));

	it("round-trips any number of frames", Promise.coroutine(function*() {
		const frameSizes = [0, 1, 2, 4, 5, 10, 200, 400, 800, 10000, 40000, 80000, 200000];
		const frames = [];
		for(const s of frameSizes) {
			frames.push(crypto.pseudoRandomBytes(s));
		}
		A(frames.length, frameSizes.length);
		const inputBuf = Buffer.concat(frames.map(makeFrame));
		const inputStream = realistic_streamifier.createReadStream(inputBuf);
		const reader = new frame_reader.Int32BufferDecoder("LE", 512 * 1024);
		utils.pipeWithErrors(inputStream, reader);
		const output = yield readableToArray(reader);
		assert.deepStrictEqual(output, frames);
	}));

	it("emits error when given a too-long frame", Promise.coroutine(function*() {
		const inputBuf = makeFrame(crypto.pseudoRandomBytes(1025));
		const inputStream = realistic_streamifier.createReadStream(inputBuf);
		const reader = new frame_reader.Int32BufferDecoder("LE", 1024);
		utils.pipeWithErrors(inputStream, reader);
		let caught = null;
		try {
			const output = yield readableToArray(reader);
		} catch(e) {
			caught = e;
		}
		A(caught instanceof frame_reader.BadData);
	}));
});
