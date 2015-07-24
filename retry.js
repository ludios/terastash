"use strong";
"use strict";

const A = require('ayy');
const T = require('notmytype');
const Promise = require('bluebird');
const utils = require('./utils');

class Decayer {
	/**
	 * initial - initial number to return
	 * multiplier - multiply number by this value after each call to decay()
	 * max - cap number at this value
	 */
	constructor(initial, multiplier, max) {
		this.initial = initial;
		this.multiplier = multiplier;
		this.max = max;
		// Inlined reset() for strong mode
		this.current = initial / multiplier;
	}

	reset() {
		// First call to .decay() will multiply, but we want to get the `intitial`
		// value on the first call to .decay(), so divide.
		this.current = this.initial / this.multiplier;
		return this.current;
	}

	decay() {
		this.current = Math.min(this.current * this.multiplier, this.max);
		return this.current;
	}
}

function wait(ms) {
	T(ms, T.number);
	A.gte(ms, 0);
	return new Promise(function(resolve) {
		setTimeout(resolve, ms);
	});
}

/**
 * func may return a Promise or a non-Promise value
 * tries is number of times to try before giving up
 * decayer is an instance of Decayer
 */
const retryFunction = Promise.coroutine(function* retryPromiseFunc$coro(func, tries, decayer) {
	T(func, T.function, tries, T.number, decayer, Decayer);
	utils.assertSafeNonNegativeInteger(tries);
	let caught = null;
	while(tries) {
		try {
			return func();
		} catch(e) {
			A(e, "expected e to be truthy");
			caught = e;
		}
		tries--;
		yield wait(decayer.decay());
	}
	A.neq(caught, null);
	throw caught;
});

module.exports = {Decayer, wait, retryFunction};
