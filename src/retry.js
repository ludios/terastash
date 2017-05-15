"use strict";

const A       = require('ayy');
const T       = require('notmytype');
const Promise = require('bluebird');
const utils   = require('./utils');

class Decayer {
	/**
	 * initial - initial number to return
	 * multiplier - multiply number by this value after each call to decay()
	 * max - cap number at this value
	 */
	constructor(initial, multiplier, max) {
		this.initial    = initial;
		this.multiplier = multiplier;
		this.max        = max;
		this.current    = this.reset()
	}

	reset() {
		// First call to .decay() will multiply, but we want to get the `intitial`
		// value on the first call to .decay(), so divide.
		this.current = this.initial / this.multiplier;
		return this.current;
	}

	// For use inside an errback where you want to tell the user how many
	// seconds the delay will be.
	getNextDelay() {
		return Math.min(this.current * this.multiplier, this.max);
	}

	decay() {
		this.current = this.getNextDelay();
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
 * func - function to call; it must return a Promise
 * errorHandler - function to call with error, if error was caught
 * tries - number of times to try before giving up
 * decayer - an instance of Decayer
 */
const retryFunction = Promise.coroutine(function* retryPromiseFunc$coro(func, errorHandler, tries, decayer) {
	T(func, T.function, errorHandler, T.function, tries, T.number, decayer, Decayer);
	utils.assertSafeNonNegativeInteger(tries);
	let caught = null;
	while(tries) {
		try {
			// Need the 'yield' here to make sure errors are caught
			// in *this* function
			return yield func();
		} catch(e) {
			A(e, "expected e to be truthy");
			caught = e;
			errorHandler(caught, tries);
		}
		tries--;
		yield wait(decayer.decay());
	}
	A.neq(caught, null);
	throw caught;
});

module.exports = {Decayer, wait, retryFunction};
