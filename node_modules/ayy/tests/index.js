"use strict";

const assert = require('assert');
const A = require('..');

describe('A', function() {
	it('works', function() {
		A(3, 3);
		assert.throws(()=>A(false), /^AyyError: A\(\.\.\.\): false not truthy$/);
		assert.throws(()=>A(false), /^AyyError: A\(\.\.\.\): false not truthy$/);
		assert.throws(()=>A(0), /^AyyError: A\(\.\.\.\): 0 not truthy$/);
		assert.throws(()=>A(""), /^AyyError: A\(\.\.\.\): '' not truthy$/);
		try {
			A(0);
		} catch(e) {
			assert.strictEqual(e.toString(), "AyyError: A(...): 0 not truthy");
		}
		try {
			A(0, "explanation");
		} catch(e) {
			assert.strictEqual(e.toString(), "AyyError: A(...): 0 not truthy: explanation");
		}
		try {
			A(0, A.fn, ()=>"explanation");
		} catch(e) {
			assert.strictEqual(e.toString(), "AyyError: A(...): 0 not truthy: explanation");
		}
		try {
			A(0, A.fn, ()=>undefined);
		} catch(e) {
			assert.strictEqual(e.toString(), "AyyError: A(...): 0 not truthy: undefined");
		}
		try {
			A(0, 0);
		} catch(e) {
			assert.strictEqual(e.toString(), "AyyError: A(...): 0 not truthy: 0");
		}
	});
});

describe('A.eq', function() {
	it('works', function() {
		A.eq(3, 3);
		try {
			A.eq(3, "3");
		} catch(e) {
			assert.strictEqual(e.toString(), "AyyError: A.eq(...): 3 !== '3'");
		}
	});
});

describe('A.neq', function() {
	it('works', function() {
		A.neq(3, "3");
		try {
			A.neq(3, 3);
		} catch(e) {
			assert.strictEqual(e.toString(), "AyyError: A.neq(...): 3 === 3");
		}
	});
});

describe('A.lt', function() {
	it('works', function() {
		A.lt(3, 4);
		try {
			A.lt(4, 3);
		} catch(e) {
			assert.strictEqual(e.toString(), "AyyError: A.lt(...): !(4 < 3)");
		}
	});
});

describe('A.lte', function() {
	it('works', function() {
		A.lte(3, 3);
		A.lte(2, 3);
		try {
			A.lte(4, 3);
		} catch(e) {
			assert.strictEqual(e.toString(), "AyyError: A.lte(...): !(4 <= 3)");
		}
	});
});

describe('A.gt', function() {
	it('works', function() {
		A.gt(4, 3);
		try {
			A.gt(3, 4);
		} catch(e) {
			assert.strictEqual(e.toString(), "AyyError: A.gt(...): !(3 > 4)");
		}
	});
});

describe('A.gte', function() {
	it('works', function() {
		A.gte(3, 3);
		A.gte(3, 2);
		try {
			A.gte(3, 4);
		} catch(e) {
			assert.strictEqual(e.toString(), "AyyError: A.gte(...): !(3 >= 4)");
		}
	});
});
