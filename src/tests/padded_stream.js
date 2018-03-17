"use strict";

require('better-buffer-inspect');

const assert        = require('assert');
const utils         = require('../utils');
const padded_stream = require('../padded_stream');
const streamifier   = require('streamifier');
const Promise       = require('bluebird');

describe('Padder', function() {
	it("pads streams when given length > stream length", async function() {
		const inputStream = streamifier.createReadStream(Buffer.from('hi'));
		const padder = new padded_stream.Padder(4);
		inputStream.pipe(padder);
		const buf = await utils.readableToBuffer(padder);
		assert.deepStrictEqual(buf, Buffer.from("hi\x00\x00"));
	});

	it("doesn't pad streams when given length < stream length", async function() {
		const inputStream = streamifier.createReadStream(Buffer.from('hi'));
		const padder = new padded_stream.Padder(1);
		inputStream.pipe(padder);
		const buf = await utils.readableToBuffer(padder);
		assert.deepStrictEqual(buf, Buffer.from("hi"));
	});

	it("doesn't pad streams when given length == stream length", async function() {
		const inputStream = streamifier.createReadStream(Buffer.from('hi'));
		const padder = new padded_stream.Padder(2);
		inputStream.pipe(padder);
		const buf = await utils.readableToBuffer(padder);
		assert.deepStrictEqual(buf, Buffer.from("hi"));
	});
});

describe('RightTruncate', function() {
	it("truncates stream when given length < stream length", async function() {
		const inputStream = streamifier.createReadStream(Buffer.from('hi\x00\x00'));
		const truncator = new padded_stream.RightTruncate(2);
		inputStream.pipe(truncator);
		const buf = await utils.readableToBuffer(truncator);
		assert.deepStrictEqual(buf, Buffer.from("hi"));
	});

	it("doesn't touch stream when given length > stream length", async function() {
		const inputStream = streamifier.createReadStream(Buffer.from('hi\x00\x00'));
		const truncator = new padded_stream.RightTruncate(10);
		inputStream.pipe(truncator);
		const buf = await utils.readableToBuffer(truncator);
		assert.deepStrictEqual(buf, Buffer.from("hi\x00\x00"));
	});

	it("doesn't touch stream when given length == stream length", async function() {
		const inputStream = streamifier.createReadStream(Buffer.from('hi\x00\x00'));
		const truncator = new padded_stream.RightTruncate(4);
		inputStream.pipe(truncator);
		const buf = await utils.readableToBuffer(truncator);
		assert.deepStrictEqual(buf, Buffer.from("hi\x00\x00"));
	});
});

describe('LeftTruncate', function() {
	it("truncates stream when given skipBytes", async function() {
		const inputStream = streamifier.createReadStream(Buffer.from('\x00\x00hi'));
		const truncator = new padded_stream.LeftTruncate(2);
		inputStream.pipe(truncator);
		const buf = await utils.readableToBuffer(truncator);
		assert.deepStrictEqual(buf, Buffer.from("hi"));
	});

	it("truncates stream to nothing when given skipBytes == stream length", async function() {
		const inputStream = streamifier.createReadStream(Buffer.from('\x00\x00hi'));
		const truncator = new padded_stream.LeftTruncate(4);
		inputStream.pipe(truncator);
		const buf = await utils.readableToBuffer(truncator);
		assert.deepStrictEqual(buf, Buffer.from(""));
	});

	it("truncates stream to nothing when given skipBytes > stream length", async function() {
		const inputStream = streamifier.createReadStream(Buffer.from('\x00\x00hi'));
		const truncator = new padded_stream.LeftTruncate(5);
		inputStream.pipe(truncator);
		const buf = await utils.readableToBuffer(truncator);
		assert.deepStrictEqual(buf, Buffer.from(""));
	});

	it("doesn't touch stream when given skipBytes == 0", async function() {
		const inputStream = streamifier.createReadStream(Buffer.from('hi\x00\x00'));
		const truncator = new padded_stream.LeftTruncate(0);
		inputStream.pipe(truncator);
		const buf = await utils.readableToBuffer(truncator);
		assert.deepStrictEqual(buf, Buffer.from("hi\x00\x00"));
	});
});
