"use strong";
"use strict";

require('better-buffer-inspect');

const assert = require('assert');
const utils = require('../utils');
const padded_stream = require('../padded_stream');
const streamifier = require('streamifier');
const Promise = require('bluebird');

describe('Padder', function() {
	it("pads streams when given length > stream length", Promise.coroutine(function*() {
		const inputStream = streamifier.createReadStream(new Buffer('hi'));
		const padder = new padded_stream.Padder(4);
		inputStream.pipe(padder);
		const buf = yield utils.streamToBuffer(padder);
		assert.deepStrictEqual(buf, new Buffer("hi\x00\x00"));
	}));

	it("doesn't pad streams when given length < stream length", Promise.coroutine(function*() {
		const inputStream = streamifier.createReadStream(new Buffer('hi'));
		const padder = new padded_stream.Padder(1);
		inputStream.pipe(padder);
		const buf = yield utils.streamToBuffer(padder);
		assert.deepStrictEqual(buf, new Buffer("hi"));
	}));

	it("doesn't pad streams when given length == stream length", Promise.coroutine(function*() {
		const inputStream = streamifier.createReadStream(new Buffer('hi'));
		const padder = new padded_stream.Padder(2);
		inputStream.pipe(padder);
		const buf = yield utils.streamToBuffer(padder);
		assert.deepStrictEqual(buf, new Buffer("hi"));
	}));
});

describe('Unpadder', function() {
	it("unpads stream when given length < stream length", Promise.coroutine(function*() {
		const inputStream = streamifier.createReadStream(new Buffer('hi\x00\x00'));
		const unpadder = new padded_stream.Unpadder(2);
		inputStream.pipe(unpadder);
		const buf = yield utils.streamToBuffer(unpadder);
		assert.deepStrictEqual(buf, new Buffer("hi"));
	}));

	it("doesn't touch stream when given length > stream length", Promise.coroutine(function*() {
		const inputStream = streamifier.createReadStream(new Buffer('hi\x00\x00'));
		const unpadder = new padded_stream.Unpadder(10);
		inputStream.pipe(unpadder);
		const buf = yield utils.streamToBuffer(unpadder);
		assert.deepStrictEqual(buf, new Buffer("hi\x00\x00"));
	}));

	it("doesn't touch stream when given length > stream length", Promise.coroutine(function*() {
		const inputStream = streamifier.createReadStream(new Buffer('hi\x00\x00'));
		const unpadder = new padded_stream.Unpadder(4);
		inputStream.pipe(unpadder);
		const buf = yield utils.streamToBuffer(unpadder);
		assert.deepStrictEqual(buf, new Buffer("hi\x00\x00"));
	}));
});
