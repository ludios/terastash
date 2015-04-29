"use strict";

const aes = require('../aes');

describe('AES self-test', function() {
	it('should not throw Error', function() {
		aes.selfTest();
	});
});
