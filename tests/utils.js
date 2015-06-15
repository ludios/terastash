"use strong";
"use strict";

require('better-buffer-inspect');

const A = require('ayy');
const utils = require('../utils');

describe('getParentPath', function() {
	it('returns the parent path', function() {
		A.eq('blah', utils.getParentPath('blah/child'));
		A.eq('', utils.getParentPath('blah'));
		A.eq('', utils.getParentPath(''));
	});
});

describe('pad', function() {
	it('pads properly when length is shorter than desired', function() {
		A.eq(' 12', utils.pad('12', 3));
		A.eq(' '.repeat(10000-2) + '12', utils.pad('12', 10000));
	});

	it('pads properly when length is equal to desired', function() {
		A.eq('123', utils.pad('123', 3));
		A.eq('123', utils.pad('123', 0));
	});

	it('pads properly when length is longer than desired', function() {
		A.eq('12345', utils.pad('12345', 3));
		A.eq('12345', utils.pad('12345', 0));
	});
});

describe('sameArrayValues', function() {
	it('compares arrays properly', function() {
		A(utils.sameArrayValues([], []));
		A(!utils.sameArrayValues([], [1]));
		A(!utils.sameArrayValues([1], []));
		A(!utils.sameArrayValues([1], ["1"]));
		A(utils.sameArrayValues(["1"], ["1"]));
		A(utils.sameArrayValues(["1", "2"], ["1", "2"]));
		A(!utils.sameArrayValues(["1", "2", "3"], ["1", "2"]));
		A(!utils.sameArrayValues(["1", "2"], ["1", "2", "3"]));
		A(!utils.sameArrayValues(["1", "2", []], ["1", "2", []]));
		A(utils.sameArrayValues([NaN], [NaN]));
		A(!utils.sameArrayValues([NaN], []));
	});
});

describe('getConcealmentSize', function() {
	it('returns the amount to round up to', function() {
		A.eq(utils.getConcealmentSize(0), 16);
		A.eq(utils.getConcealmentSize(1), 16);
		A.eq(utils.getConcealmentSize(128), 16);
		A.eq(utils.getConcealmentSize(256), 16);
		A.eq(utils.getConcealmentSize(1024), 16);
		A.eq(utils.getConcealmentSize(1.5*1024), 16);
		A.eq(utils.getConcealmentSize(2*1024), 32);
		A.eq(utils.getConcealmentSize(128*1024), 2048);

		A.eq(utils.getConcealmentSize(1024), 1024/64);

		A.eq(utils.getConcealmentSize(1024*1024), 1024*1024/64);

		A.eq(utils.getConcealmentSize(1024*1024*1024 - 1), 1024*1024*1024/128);
		A.eq(utils.getConcealmentSize(1024*1024*1024), 1024*1024*1024/64);
		A.eq(utils.getConcealmentSize(1024*1024*1024 + 1), 1024*1024*1024/64);
		A.eq(utils.getConcealmentSize(1024*1024*1024 + 1024*1024), 1024*1024*1024/64);
	});
});


describe('concealSize', function() {
	it('returns the concealed file size', function() {
		A.eq(utils.concealSize(0), 16);
		A.eq(utils.concealSize(1), 16);
		A.eq(utils.concealSize(128), 128);
		A.eq(utils.concealSize(256), 256);
		A.eq(utils.concealSize(1024), 1024);
		A.eq(utils.concealSize(1025), 1024 + 16);
		A.eq(utils.concealSize(1.5*1024), 1.5*1024);
		A.eq(utils.concealSize(2*1024), 2*1024);
		A.eq(utils.concealSize(2*1024+1), 2*1024 + 32);

		A.eq(utils.concealSize(1024*1024*1024 - 1), 1024*1024*1024);
		A.eq(utils.concealSize(1024*1024*1024), 1024*1024*1024);
		A.eq(utils.concealSize(1024*1024*1024 + 1), 1024*1024*1024 + 1024*1024*1024/64);
		A.eq(utils.concealSize(1024*1024*1024 + 1024*1024), 1024*1024*1024 + 1024*1024*1024/64);
	});
});

describe('allIdentical', function() {
	it('works', function() {
		A.eq(utils.allIdentical([]), true);
		A.eq(utils.allIdentical([1]), true);
		A.eq(utils.allIdentical([1, 1]), true);
		A.eq(utils.allIdentical([1, 1, 1]), true);
		A.eq(utils.allIdentical([1, "1"]), false);
		A.eq(utils.allIdentical(["1", 1]), false);
		A.eq(utils.allIdentical([1, "1", 1]), false);
		A.eq(utils.allIdentical([1, "1", 1, "1", 1, "1"]), false);
		A.eq(utils.allIdentical([1, "1", 1, "1", 1, "1", 1]), false);
	});
});
