"use strong";
"use strict";

require('better-buffer-inspect');

const utils = require('../utils');
const assert = require('assert');

describe('getParentPath', function() {
	it('returns the parent path', function() {
		assert.equal('blah', utils.getParentPath('blah/child'));
		assert.equal('', utils.getParentPath('blah'));
		assert.equal('', utils.getParentPath(''));
	});
});

describe('pad', function() {
	it('pads properly when length is shorter than desired', function() {
		assert.equal(' 12', utils.pad('12', 3));
		assert.equal(' '.repeat(10000-2) + '12', utils.pad('12', 10000));
	});

	it('pads properly when length is equal to desired', function() {
		assert.equal('123', utils.pad('123', 3));
		assert.equal('123', utils.pad('123', 0));
	});

	it('pads properly when length is longer than desired', function() {
		assert.equal('12345', utils.pad('12345', 3));
		assert.equal('12345', utils.pad('12345', 0));
	});
});

describe('sameArrayValues', function() {
	it('compares arrays properly', function() {
		assert(utils.sameArrayValues([], []));
		assert(!utils.sameArrayValues([], [1]));
		assert(!utils.sameArrayValues([1], []));
		assert(!utils.sameArrayValues([1], ["1"]));
		assert(utils.sameArrayValues(["1"], ["1"]));
		assert(utils.sameArrayValues(["1", "2"], ["1", "2"]));
		assert(!utils.sameArrayValues(["1", "2", "3"], ["1", "2"]));
		assert(!utils.sameArrayValues(["1", "2"], ["1", "2", "3"]));
		assert(!utils.sameArrayValues(["1", "2", []], ["1", "2", []]));
		assert(utils.sameArrayValues([NaN], [NaN]));
		assert(!utils.sameArrayValues([NaN], []));
	});
});

describe('getConcealmentSize', function() {
	it('returns the amount to round up to', function() {
		assert.equal(utils.getConcealmentSize(0), 16);
		assert.equal(utils.getConcealmentSize(1), 16);
		assert.equal(utils.getConcealmentSize(128), 16);
		assert.equal(utils.getConcealmentSize(256), 16);
		assert.equal(utils.getConcealmentSize(1024), 16);
		assert.equal(utils.getConcealmentSize(1.5*1024), 16);
		assert.equal(utils.getConcealmentSize(2*1024), 32);
		assert.equal(utils.getConcealmentSize(128*1024), 2048);

		assert.equal(utils.getConcealmentSize(1024), 1024/64);

		assert.equal(utils.getConcealmentSize(1024*1024), 1024*1024/64);

		assert.equal(utils.getConcealmentSize(1024*1024*1024 - 1), 1024*1024*1024/128);
		assert.equal(utils.getConcealmentSize(1024*1024*1024), 1024*1024*1024/64);
		assert.equal(utils.getConcealmentSize(1024*1024*1024 + 1), 1024*1024*1024/64);
		assert.equal(utils.getConcealmentSize(1024*1024*1024 + 1024*1024), 1024*1024*1024/64);
	});
});


describe('concealSize', function() {
	it('returns the concealed file size', function() {
		assert.equal(utils.concealSize(0), 16);
		assert.equal(utils.concealSize(1), 16);
		assert.equal(utils.concealSize(128), 128);
		assert.equal(utils.concealSize(256), 256);
		assert.equal(utils.concealSize(1024), 1024);
		assert.equal(utils.concealSize(1025), 1024 + 16);
		assert.equal(utils.concealSize(1.5*1024), 1.5*1024);
		assert.equal(utils.concealSize(2*1024), 2*1024);
		assert.equal(utils.concealSize(2*1024+1), 2*1024 + 32);

		assert.equal(utils.concealSize(1024*1024*1024 - 1), 1024*1024*1024);
		assert.equal(utils.concealSize(1024*1024*1024), 1024*1024*1024);
		assert.equal(utils.concealSize(1024*1024*1024 + 1), 1024*1024*1024 + 1024*1024*1024/64);
		assert.equal(utils.concealSize(1024*1024*1024 + 1024*1024), 1024*1024*1024 + 1024*1024*1024/64);
	});
});
