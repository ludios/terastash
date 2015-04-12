const terastash = require('..');
const assert = require('assert');

describe('getParentPath', function() {
	it('should return the parent path', function() {
		assert.equal('/blah', terastash.getParentPath('/blah/child'))
	});
});

describe('pad', function() {
	it('should pad properly when length is shorter than desired', function() {
		assert.equal(' 12', terastash.pad('12', 3));
		assert.equal(' '.repeat(10000-2) + '12', terastash.pad('12', 10000));
	});

	it('should pad properly when length is equal to desired', function() {
		assert.equal('123', terastash.pad('123', 3));
		assert.equal('123', terastash.pad('123', 0));
	});

	it('should pad properly when length is longer than desired', function() {
		assert.equal('12345', terastash.pad('12345', 3));
		assert.equal('12345', terastash.pad('12345', 0));
	});
})
