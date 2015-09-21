"use strong";
"use strict";

require('better-buffer-inspect');

const assert = require('assert');
const A = require('ayy');
const T = require('notmytype');
const utils = require('../utils');
const frame_reader = require('../frame_reader');
const realistic_streamifier = require('../realistic_streamifier');
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
		const outputBuf = yield readableToArray(reader);
		A.eq(outputBuf.length, 0);
	}));
});
