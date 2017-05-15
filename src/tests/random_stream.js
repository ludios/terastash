"use strict";

require('better-buffer-inspect');

const A = require('ayy');
const utils = require('../utils');
const Promise = require('bluebird');
const random_stream = require('../random_stream');

describe('SecureRandomStream', function() {
	it("generates the right amount of random", Promise.coroutine(function*() {
		for(const length of [0, 1, 2, 4096, 64 * 1024 - 1, 64 * 1024, 64 * 1024 + 1, 1024 * 1024, Math.floor(Math.random() * (1024 * 1024))]) {
			//console.error(length);
			const buf = yield utils.readableToBuffer(new random_stream.SecureRandomStream(length));
			A.eq(buf.length, length);
		}
	}));
});
