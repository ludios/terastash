"use strong";
"use strict";

require('better-buffer-inspect');

const assert = require('assert');
const terastash = require('../');

function makeChunks(sizes) {
	const chunks = [];
	let idx = 0;
	for(const s of sizes) {
		chunks.push({size: s, idx: idx, file_id: "", crc32c: new Buffer(4).fill(0)});
		idx++;
	}
	return chunks;
}

describe('terastash.chunksToBlockRanges()', function() {
	it('returns block-indexed ranges', function() {
		assert.deepStrictEqual(terastash.chunksToBlockRanges(makeChunks([100, 100]), 1), [[0, 100], [100, 200]]);
		assert.deepStrictEqual(terastash.chunksToBlockRanges(makeChunks([100, 100]), 10), [[0, 10], [10, 20]]);
		assert.deepStrictEqual(terastash.chunksToBlockRanges(makeChunks([100, 1000, 100]), 10), [[0, 10], [10, 110], [110, 120]]);
	});
});
