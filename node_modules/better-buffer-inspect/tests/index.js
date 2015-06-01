"use strong";
"use strict";

require('..');
const assert = require('assert');
const inspect = require('util').inspect;

describe('Buffer.prototype.inspect', function() {
	it('shows 0-length Buffers', function() {
		const buf = new Buffer(0);
		assert.equal(inspect(buf), '<Buffer size=0>');
	});

	it('shows undecodeable Buffers as bytes', function() {
		const buf = new Buffer(4).fill(255);
		assert.equal(inspect(buf), '<Buffer size=4: ff ff ff ff>');
	});

	it('shows decodeable but too-long Buffers as bytes', function() {
		const buf = new Buffer(1000).fill(0);
		assert.equal(inspect(buf), '<Buffer size=1000: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 ... >');
	});

	it('shows decodeable Buffers as utf-8 strings', function() {
		assert.equal(inspect(new Buffer("hello world\n")), "<Buffer size=12: utf8 'hello world\\n'>");
		assert.equal(inspect(new Buffer(2).fill(0)), "<Buffer size=2: utf8 '\\u0000\\u0000'>");
		assert.equal(inspect(new Buffer("\ucccc", "utf8")), "<Buffer size=3: utf8 '\ucccc'>");
	});
});
