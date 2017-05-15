"use strict";

require('better-buffer-inspect');

const A = require('ayy');
const assert = require('assert');
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

describe('utils.splitString()', function() {
	it('without a maxsplit argument', function() {
		assert.deepStrictEqual(["", "ello"], utils.splitString("hello", "h"));
		assert.deepStrictEqual(["", ""], utils.splitString("hello", "hello"));
		assert.deepStrictEqual(["", "ello", "ello"], utils.splitString("hellohello", "h"));
		assert.deepStrictEqual(["1", "2", "3"], utils.splitString("1xy2xy3", "xy"));
	});

	it('with a maxsplit argument', function() {
		assert.deepStrictEqual(["one", "two_three"], utils.splitString("one_two_three", "_", 1));
		assert.deepStrictEqual(["1", "2", "3", "4"], utils.splitString("1_2_3_4", "_", 3));
		assert.deepStrictEqual(["1", "2", "3", "4_5"], utils.splitString("1_2_3_4_5", "_", 3));
		assert.deepStrictEqual(["1", "2", "3", "4__5"], utils.splitString("1__2__3__4__5", "__", 3));
	});

	it('with a maxsplit resulting in an empty string', function() {
		assert.deepStrictEqual(["hello"], utils.splitString("hello", "_", 1));
		assert.deepStrictEqual(["hello", ""], utils.splitString("hello_", "_", 1));
		assert.deepStrictEqual(["hello", "world", ""], utils.splitString("hello_world_", "_", 2));
		assert.deepStrictEqual(["hello", "world_"], utils.splitString("hello_world_", "_", 1));
	});

	it('with maxsplit 0', function() {
		assert.deepStrictEqual(["hello"], utils.splitString("hello", "h", 0));
		assert.deepStrictEqual(["1x2x3"], utils.splitString("1x2x3", "x", 0));
	});

	it('works like Python when given negative maxsplit', function() {
		// Numbers less than 0 act like not passing in a C{maxsplit}
		assert.deepStrictEqual(["", "ello"], utils.splitString("hello", "h", -1));
		assert.deepStrictEqual(["xx", "yy", "zz"], utils.splitString("xx_yy_zz", "_", -1));
		assert.deepStrictEqual(["xx", "yy", "zz"], utils.splitString("xx_yy_zz", "_", -2));
		assert.deepStrictEqual(["xx", "yy", "zz"], utils.splitString("xx_yy_zz", "_", -3));
	});
});

describe('utils.rsplitString()', function() {
	it('without a maxsplit argument', function() {
		assert.deepStrictEqual(["", "ello"], utils.rsplitString("hello", "h"));
		assert.deepStrictEqual(["", ""], utils.rsplitString("hello", "hello"));
		assert.deepStrictEqual(["", "ello", "ello"], utils.rsplitString("hellohello", "h"));
		assert.deepStrictEqual(["1", "2", "3"], utils.rsplitString("1xy2xy3", "xy"));
	});

	it('with a maxsplit argument', function() {
		assert.deepStrictEqual(["one_two", "three"], utils.rsplitString("one_two_three", "_", 1));
		assert.deepStrictEqual(["1", "2", "3", "4"], utils.rsplitString("1_2_3_4", "_", 3));
		assert.deepStrictEqual(["1_2", "3", "4", "5"], utils.rsplitString("1_2_3_4_5", "_", 3));
		assert.deepStrictEqual(["1__2", "3", "4", "5"], utils.rsplitString("1__2__3__4__5", "__", 3));
	});

	it('with a maxsplit resulting in an empty string', function() {
		assert.deepStrictEqual(["hello"], utils.rsplitString("hello", "_", 1));
		assert.deepStrictEqual(["hello", ""], utils.rsplitString("hello_", "_", 1));
		assert.deepStrictEqual(["hello", "world", ""], utils.rsplitString("hello_world_", "_", 2));
		assert.deepStrictEqual(["hello_world", ""], utils.rsplitString("hello_world_", "_", 1));
	});

	it('with maxsplit 0', function() {
		assert.deepStrictEqual(["hello"], utils.rsplitString("hello", "h", 0));
		assert.deepStrictEqual(["1x2x3"], utils.rsplitString("1x2x3", "x", 0));
	});

	it('works like Python when given negative maxsplit', function() {
		// Numbers less than 0 act like not passing in a C{maxsplit}
		assert.deepStrictEqual(["", "ello"], utils.rsplitString("hello", "h", -1));
		assert.deepStrictEqual(["xx", "yy", "zz"], utils.rsplitString("xx_yy_zz", "_", -1));
		assert.deepStrictEqual(["xx", "yy", "zz"], utils.rsplitString("xx_yy_zz", "_", -2));
		assert.deepStrictEqual(["xx", "yy", "zz"], utils.rsplitString("xx_yy_zz", "_", -3));
	});
});

describe('utils.intersect()', function() {
	it('finds intersection of two ranges', function() {
		assert.deepStrictEqual(utils.intersect([0, 100], [0, 100]), [0, 100]);
		assert.deepStrictEqual(utils.intersect([0, 1], [0, 2]), [0, 1]);
		assert.deepStrictEqual(utils.intersect([0, 100], [1, 100]), [1, 100]);
		assert.deepStrictEqual(utils.intersect([0, 100], [50, 150]), [50, 100]);
		assert.deepStrictEqual(utils.intersect([50, 150], [0, 100]), [50, 100]);
		assert.deepStrictEqual(utils.intersect([100, 200], [50, 150]), [100, 150]);
		assert.deepStrictEqual(utils.intersect([50, 150], [100, 200]), [100, 150]);
		assert.deepStrictEqual(utils.intersect([200, 300], [50, 150]), null);
		assert.deepStrictEqual(utils.intersect([50, 150], [200, 300]), null);
	});
});

describe('utils.zip()', function() {
	function* yielder() {
		yield 7;
		yield 8;
		yield 9;
	}

	it('zips things', function() {
		assert.deepStrictEqual(Array.from(utils.zip([1, 2, 3], [4, 5, 6])), [[1, 4], [2, 5], [3, 6]]);
		assert.deepStrictEqual(Array.from(utils.zip([1, 2, 3], [4, 5, 6, 7])), [[1, 4], [2, 5], [3, 6]]);
		assert.deepStrictEqual(Array.from(utils.zip([1, 2, 3], [4, 5])), [[1, 4], [2, 5], [3, undefined]]);
		assert.deepStrictEqual(Array.from(utils.zip()), []);
		assert.deepStrictEqual(Array.from(utils.zip([1, 2, 3])), [[1], [2], [3]]);
		assert.deepStrictEqual(Array.from(utils.zip([1, 2, 3], [4, 5, 6], [7, 8, 9])), [[1, 4, 7], [2, 5, 8], [3, 6, 9]]);
		assert.deepStrictEqual(Array.from(utils.zip([1, 2, 3], [4, 5, 6], yielder())), [[1, 4, 7], [2, 5, 8], [3, 6, 9]]);
	});
});

describe('utils.shuffleArray()', function() {
	it('returns the same Array if of length 0', function() {
		assert.deepStrictEqual(utils.shuffleArray([]), []);
	});

	it('returns the same Array if of length 1', function() {
		assert.deepStrictEqual(utils.shuffleArray([3]), [3]);
	});

	it('returns a shuffled Array', function() {
		const shuffled = utils.shuffleArray([3, 4]);
		try {
			assert.deepStrictEqual(shuffled, [3, 4]);
		} catch(e) {
			assert.deepStrictEqual(shuffled, [4, 3]);
		}
	});
});
