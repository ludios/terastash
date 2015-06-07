"use strong";
"use strict";

require('better-buffer-inspect');

const A = require('ayy');
const terastash = require('..');
const fs = require('fs');
const os = require('os');
const crypto = require('crypto');
const path = require('path');
const utils = require('../utils');
const Promise = require('bluebird');
const gdrive = require('../chunker/gdrive');

describe('GDriver', function() {
	it('can upload a file, create folder', Promise.coroutine(function*() {
		this.timeout(20000);

		const config = yield terastash.getChunkStores();
		const chunkStore = config.stores["terastash-tests-gdrive"];
		if(!chunkStore) {
			throw new Error("Please define a terastash-tests-gdrive chunk store to run this test");
		}
		A.eq(chunkStore.type, "gdrive");
		const gdriver = new gdrive.GDriver(chunkStore.clientId, chunkStore.clientSecret);
		yield gdriver.loadCredentials();

		const tempFname = path.join(os.tmpdir(), 'terastash-gdrive-tests-' + Math.random());
		const buf = crypto.pseudoRandomBytes(utils.randInt(0, 5*1024));
		yield utils.writeFileAsync(tempFname, buf, 0, buf.length);

		const result = yield gdriver.createFile("test-file", {parents: chunkStore.parents}, fs.createReadStream(tempFname));
		A.eq(typeof result.id, "string");

		const result2 = yield gdriver.createFolder("test-folder", {parents: chunkStore.parents});
		A.eq(typeof result2.id, "string");
		//console.log(`Created folder with id ${result2.id}`);
	}));
});
