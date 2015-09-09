"use strict";

const T = require('notmytype');

function object(...args) {
	const [existing] = args;
	T(existing, T.object);
	const obj = {};
	if(existing) {
		Object.assign(obj, existing);
	}
	return obj;
}

module.exports = {object};
