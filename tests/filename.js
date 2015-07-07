"use strong";
"use strict";

const filename = require('../filename');
const assert = require('assert');

describe('.check()', function() {
	it("throws Error for illegal filenames", function() {
		assert.throws(() => filename.check("x/y"), /^BadFilename:.*cannot contain '\/'/);
		assert.throws(() => filename.check("x\x00y"), /^BadFilename:.*cannot contain NULL/);
		assert.throws(() => filename.check(""), /^BadFilename:.*cannot be '', '\.', or '\.\.'/);
		assert.throws(() => filename.check("."), /^BadFilename:.*cannot be '', '\.', or '\.\.'/);
		assert.throws(() => filename.check(".."), /^BadFilename:.*cannot be '', '\.', or '\.\.'/);
		assert.throws(() => filename.check(" "), /^BadFilename:.*cannot be '', '\.', or '\.\.'/);
		assert.throws(() => filename.check(" . "), /^BadFilename:.*cannot be '', '\.', or '\.\.'/);
		assert.throws(() => filename.check(" .. "), /^BadFilename:.*cannot be '', '\.', or '\.\.'/);
		assert.throws(() => filename.check("hello."), /^BadFilename: Windows shell does not support filenames that end with '\.'/);
		assert.throws(() => filename.check("hello "), /^BadFilename: Windows shell does not support filenames that end with space/);
		assert.throws(() => filename.check("con"), /^BadFilename: .*not support filenames whose non-extension component is /);
		assert.throws(() => filename.check("con.c"), /^BadFilename: .*not support filenames whose non-extension component is /);
		assert.throws(() => filename.check("con.c.last"), /^BadFilename: .*not support filenames whose non-extension component is /);
		assert.throws(() => filename.check("COM7"), /^BadFilename: .*not support filenames whose non-extension component is /);
		assert.throws(() => filename.check("COM7.c"), /^BadFilename: .*not support filenames whose non-extension component is /);
		assert.throws(() => filename.check("COM7.c.last"), /^BadFilename: .*not support filenames whose non-extension component is /);
		assert.throws(() => filename.check("lpt9"), /^BadFilename: .*not support filenames whose non-extension component is /);
		assert.throws(() => filename.check("lpt9.c"), /^BadFilename: .*not support filenames whose non-extension component is /);
		assert.throws(() => filename.check("lpt9.c.last"), /^BadFilename: .*not support filenames whose non-extension component is /);
	});

	it("doesn't throw Error for legal filenames", function() {
		assert.doesNotThrow(() => filename.check("hello"));
		assert.doesNotThrow(() => filename.check("hello world"));
		assert.doesNotThrow(() => filename.check("hello\uccccworld"));
	});
});
