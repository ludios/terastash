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
		const buf = yield utils.readableToBuffer(padder);
		assert.deepStrictEqual(buf, new Buffer("hi\x00\x00"));
	}));

	it("doesn't pad streams when given length < stream length", Promise.coroutine(function*() {
		const inputStream = streamifier.createReadStream(new Buffer('hi'));
		const padder = new padded_stream.Padder(1);
		inputStream.pipe(padder);
		const buf = yield utils.readableToBuffer(padder);
		assert.deepStrictEqual(buf, new Buffer("hi"));
	}));

	it("doesn't pad streams when given length == stream length", Promise.coroutine(function*() {
		const inputStream = streamifier.createReadStream(new Buffer('hi'));
		const padder = new padded_stream.Padder(2);
		inputStream.pipe(padder);
		const buf = yield utils.readableToBuffer(padder);
		assert.deepStrictEqual(buf, new Buffer("hi"));
	}));
});

describe('RightTruncate', function() {
	it("truncates stream when given length < stream length", Promise.coroutine(function*() {
		const inputStream = streamifier.createReadStream(new Buffer('hi\x00\x00'));
		const truncator = new padded_stream.RightTruncate(2);
		inputStream.pipe(truncator);
		const buf = yield utils.readableToBuffer(truncator);
		assert.deepStrictEqual(buf, new Buffer("hi"));
	}));

	it("doesn't touch stream when given length > stream length", Promise.coroutine(function*() {
		const inputStream = streamifier.createReadStream(new Buffer('hi\x00\x00'));
		const truncator = new padded_stream.RightTruncate(10);
		inputStream.pipe(truncator);
		const buf = yield utils.readableToBuffer(truncator);
		assert.deepStrictEqual(buf, new Buffer("hi\x00\x00"));
	}));

	it("doesn't touch stream when given length == stream length", Promise.coroutine(function*() {
		const inputStream = streamifier.createReadStream(new Buffer('hi\x00\x00'));
		const truncator = new padded_stream.RightTruncate(4);
		inputStream.pipe(truncator);
		const buf = yield utils.readableToBuffer(truncator);
		assert.deepStrictEqual(buf, new Buffer("hi\x00\x00"));
	}));
});

describe('LeftTruncate', function() {
	it("truncates stream when given skipBytes", Promise.coroutine(function*() {
		const inputStream = streamifier.createReadStream(new Buffer('\x00\x00hi'));
		const truncator = new padded_stream.LeftTruncate(2);
		inputStream.pipe(truncator);
		const buf = yield utils.readableToBuffer(truncator);
		assert.deepStrictEqual(buf, new Buffer("hi"));
	}));

	it("truncates stream to nothing when given skipBytes == stream length", Promise.coroutine(function*() {
		const inputStream = streamifier.createReadStream(new Buffer('\x00\x00hi'));
		const truncator = new padded_stream.LeftTruncate(4);
		inputStream.pipe(truncator);
		const buf = yield utils.readableToBuffer(truncator);
		assert.deepStrictEqual(buf, new Buffer(""));
	}));

	it("truncates stream to nothing when given skipBytes > stream length", Promise.coroutine(function*() {
		const inputStream = streamifier.createReadStream(new Buffer('\x00\x00hi'));
		const truncator = new padded_stream.LeftTruncate(5);
		inputStream.pipe(truncator);
		const buf = yield utils.readableToBuffer(truncator);
		assert.deepStrictEqual(buf, new Buffer(""));
	}));

	it("doesn't touch stream when given skipBytes == 0", Promise.coroutine(function*() {
		const inputStream = streamifier.createReadStream(new Buffer('hi\x00\x00'));
		const truncator = new padded_stream.LeftTruncate(0);
		inputStream.pipe(truncator);
		const buf = yield utils.readableToBuffer(truncator);
		assert.deepStrictEqual(buf, new Buffer("hi\x00\x00"));
	}));
});
