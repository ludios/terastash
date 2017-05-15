"use strict";

const assert = require('assert');
const aes = require('../aes');

describe('aes.selfTest()', function() {
	it('does not throw Error', function() {
		aes.selfTest();
	});
});

describe('aes.blockNumberToIv()', function() {
	it('returns correct results', function() {
		assert.deepStrictEqual(
			aes.blockNumberToIv(0),
			Buffer.from('00000000000000000000000000000000', 'hex')
		);
		assert.deepStrictEqual(
			aes.blockNumberToIv(1),
			Buffer.from('00000000000000000000000000000001', 'hex')
		);
		assert.deepStrictEqual(
			aes.blockNumberToIv(100),
			Buffer.from('00000000000000000000000000000064', 'hex')
		);
		assert.deepStrictEqual(
			aes.blockNumberToIv(Math.pow(2, 53) - 1),
			Buffer.from('0000000000000000001fffffffffffff', 'hex')
		);
	});
});
