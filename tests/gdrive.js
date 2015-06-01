"use strong";
"use strict";

require('better-buffer-inspect');

const terastash = require('..');
const fs = require('fs');
const os = require('os');
const crypto = require('crypto');
const path = require('path');
const utils = require('../utils');
const Promise = require('bluebird');
const gdrive = require('../chunker/gdrive');
const assert = require('assert');

describe('GDriver', function() {
	it('can upload a file', Promise.coroutine(function*() {
		this.timeout = 8000;

		const config = yield terastash.getChunkStores();
		const chunkStore = config.stores["terastash-tests-gdrive"];
		if(!chunkStore) {
			throw new Error("Please define a terastash-tests-gdrive chunk store to run this test");
		}
		assert.strictEqual(chunkStore.type, "gdrive");
		const gdriver = new gdrive.GDriver(chunkStore.clientId, chunkStore.clientSecret);
		yield gdriver.loadCredentials();

		const tempFname = path.join(os.tmpdir(), 'terastash-gdrive-tests-' + Math.random());
		const buf = crypto.pseudoRandomBytes(4 * 1024);
		yield utils.writeFileAsync(tempFname, buf, 0, buf.length);

		const result = yield gdriver.createFile("test-file", {parents: chunkStore.parents}, fs.createReadStream(tempFname));
		assert.strictEqual(typeof result.id, "string");
	}));
});
