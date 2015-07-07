"use strong";
"use strict";

const filename = require('../filename');
const assert = require('assert');

describe('filename.check', function() {
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
		assert.throws(() => filename.check("hello\\world"), /^BadFilename: .*not support filenames that contain /);
		assert.throws(() => filename.check("hello:world"), /^BadFilename: .*not support filenames that contain /);
		assert.throws(() => filename.check("hello?world"), /^BadFilename: .*not support filenames that contain /);
		assert.throws(() => filename.check("hello>world"), /^BadFilename: .*not support filenames that contain /);
		assert.throws(() => filename.check("hello<world"), /^BadFilename: .*not support filenames that contain /);
		assert.throws(() => filename.check("hello|world"), /^BadFilename: .*not support filenames that contain /);
		assert.throws(() => filename.check("hello\"world"), /^BadFilename: .*not support filenames that contain /);
		assert.throws(() => filename.check("hello\*world"), /^BadFilename: .*not support filenames that contain /);
		assert.throws(() => filename.check("hello\x01world"), /^BadFilename: .*not support filenames that contain /);
		assert.throws(() => filename.check("hello\nworld"), /^BadFilename: .*not support filenames that contain /);
		assert.throws(() => filename.check("hello\x1Fworld"), /^BadFilename: .*not support filenames that contain /);
		assert.throws(() => filename.check("\ucccc".repeat(256)), /^BadFilename: .*not support filenames with > 255 characters/);
		assert.throws(() => filename.check("\ucccc".repeat(128)), /^BadFilename: .*not support filenames with > 255 bytes/);
		assert.throws(() => filename.check("hello\u200cworld"), /^BadFilename: .*one or more codepoints that are ignorable on HFS/);
		assert.throws(() => filename.check("hello\u206fworld"), /^BadFilename: .*one or more codepoints that are ignorable on HFS/);
		assert.throws(() => filename.check("hello\ufeffworld"), /^BadFilename: .*one or more codepoints that are ignorable on HFS/);
	});

	it("doesn't throw Error for legal filenames", function() {
		assert.strictEqual(filename.check("hello"), "hello");
		assert.strictEqual(filename.check("hello world"), "hello world");
		assert.strictEqual(filename.check("hello\uccccworld"), "hello\uccccworld");
		assert.strictEqual(filename.check("h".repeat(255)), "h".repeat(255));
	});
});
