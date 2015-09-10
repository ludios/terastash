"use strong";
"use strict";

const A = require('ayy');
const T = require('notmytype');
const crypto = require('crypto');
const utils = require('./utils');

const BLOCK_SIZE = 16;
const IV_SIZE = 16;

function strictZeroPad(s, num) {
	T(s, T.string, num, T.number);
	utils.assertSafeNonNegativeInteger(num);
	A.lte(s.length, num);
	return '0'.repeat(num - s.length) + s;
}

function blockNumberToIv(blockNum) {
	utils.assertSafeNonNegativeInteger(blockNum);
	const buf = new Buffer(strictZeroPad(blockNum.toString(16), IV_SIZE * 2), 'hex');
	A(buf.length, IV_SIZE);
	return buf;
}

/**
 * Make sure that OpenSSL's AES-128-CTR cipher works as expected, including
 * our use of the IV as the counter.
 */
function selfTest() {
	let cipher;
	let decrypted;
	const key = new Buffer('12300000000000045600000000000789', 'hex');
	const iv0 = blockNumberToIv(0);
	const iv1 = blockNumberToIv(1);

	// Test for exact ciphertext
	cipher = crypto.createCipheriv('aes-128-ctr', key, iv0);
	const text = 'Hello, world. This is a test string spanning multiple AES blocks.';
	const encrypted = cipher.update(new Buffer(text));
	A.eq(
		encrypted.toString('hex'),
		'5a4be59fb050aa6059075162597141e2ff2c99e3b7b968f3396d50712587640626719d' +
		'c348cb5d966985eb7bb964e35bbe0dd77624386b875f46694a1e89b49ec2'
	);

	// Test that encryption->decryption round-trips
	cipher = crypto.createCipheriv('aes-128-ctr', key, iv0);
	decrypted = cipher.update(encrypted);
	A.eq(decrypted.toString('utf-8'), text);

	// Test that we can decrypt the middle of the ciphertext with an incremented IV
	cipher = crypto.createCipheriv('aes-128-ctr', key, iv1);
	decrypted = cipher.update(encrypted.slice(BLOCK_SIZE));
	A.eq(decrypted.toString('utf-8'), text.substr(BLOCK_SIZE));
}

module.exports = {BLOCK_SIZE, strictZeroPad, blockNumberToIv, selfTest};
