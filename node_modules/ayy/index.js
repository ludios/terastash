"use strict";

const inspect = require('util').inspect;

class AyyError extends Error {
	constructor(message, stackStartFunction) {
		super();
		this.name = 'AyyError';
		this.message = message;
		Error.captureStackTrace(this, stackStartFunction);
	}
}

const fn = Symbol("A.fn");

function _appendExtraMessage(message, extraMessage, messageFn) {
	if(extraMessage === fn) {
		// If function returns undefined, append value anyway
		message += `: ${messageFn()}`;
	} else if(extraMessage !== undefined) {
		// We might prefer to check for arg count instead
		// of checking for `undefined`, but using ...args would
		// probably be slower.
		message += `: ${extraMessage}`;
	}
	return message;
}

function A(value, extraMessage, messageFn) {
	if(!value) {
		const message = _appendExtraMessage(
			`A(...): ${inspect(value)} not truthy`, extraMessage, messageFn);
		throw new AyyError(message, A);
	}
}

A.eq = function eq(a, b, extraMessage, messageFn) {
	if(a !== b) {
		const message = _appendExtraMessage(
			`A.eq(...): ${inspect(a)} !== ${inspect(b)}`, extraMessage, messageFn);
		throw new AyyError(message, A.eq);
	}
};

A.neq = function neq(a, b, extraMessage, messageFn) {
	if(a === b) {
		const message = _appendExtraMessage(
			`A.neq(...): ${inspect(a)} === ${inspect(b)}`, extraMessage, messageFn);
		throw new AyyError(message, A.neq);
	}
};

A.lt = function lt(a, b, extraMessage, messageFn) {
	if(!(a < b)) {
		const message = _appendExtraMessage(
			`A.lt(...): !(${inspect(a)} < ${inspect(b)})`, extraMessage, messageFn);
		throw new AyyError(message, A.lt);
	}
};

A.lte = function lte(a, b, extraMessage, messageFn) {
	if(!(a <= b)) {
		const message = _appendExtraMessage(
			`A.lte(...): !(${inspect(a)} <= ${inspect(b)})`, extraMessage, messageFn);
		throw new AyyError(message, A.lte);
	}
};

A.gt = function gt(a, b, extraMessage, messageFn) {
	if(!(a > b)) {
		const message = _appendExtraMessage(
			`A.gt(...): !(${inspect(a)} > ${inspect(b)})`, extraMessage, messageFn);
		throw new AyyError(message, A.gt);
	}
};

A.gte = function gte(a, b, extraMessage, messageFn) {
	if(!(a >= b)) {
		const message = _appendExtraMessage(
			`A.gte(...): !(${inspect(a)} >= ${inspect(b)})`, extraMessage, messageFn);
		throw new AyyError(message, A.gte);
	}
};

A.fn = fn;
A.AyyError = AyyError;
module.exports = A;
