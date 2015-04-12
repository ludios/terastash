const terastash = require('..');
const assert = require('assert');

describe('getParentPath', function() {
	it('should return the parent path', function() {
		assert.equal('/blah', terastash.getParentPath('/blah/child'))
	});
});
