"use strong";
"use strict";

const assert = require('assert');
const gcmer = require('../gcmer');

describe('gcmer.blockNumberToIv()', function() {
	it('returns correct results', function() {
		assert.deepStrictEqual(
			gcmer.blockNumberToIv(0),
			new Buffer('000000000000000000000000', 'hex')
		);
		assert.deepStrictEqual(
			gcmer.blockNumberToIv(1),
			new Buffer('000000000000000000000001', 'hex')
		);
		assert.deepStrictEqual(
			gcmer.blockNumberToIv(100),
			new Buffer('000000000000000000000064', 'hex')
		);
		assert.deepStrictEqual(
			gcmer.blockNumberToIv(Math.pow(2, 53) - 1),
			new Buffer('00000000001fffffffffffff', 'hex')
		);
	});
});
