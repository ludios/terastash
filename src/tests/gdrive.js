"use strict";

require('better-buffer-inspect');

const assert    = require('assert');
const A         = require('ayy');
const terastash = require('..');
const fs        = require('../fs-promisified');
const os        = require('os');
const crypto    = require('crypto');
const path      = require('path');
const utils     = require('../utils');
const gdrive    = require('../chunker/gdrive');

async function doSetup() {
	const config = await terastash.getChunkStores();
	const chunkStore = config.stores["terastash-tests-gdrive"];
	if(!chunkStore) {
		throw new Error("Please define a terastash-tests-gdrive chunk store to run this test");
	}
	A.eq(chunkStore.type, "gdrive");
	const gdriver = new gdrive.GDriver(chunkStore.clientId, chunkStore.clientSecret);
	await gdriver.loadCredentials(gdrive.pickRandomAccount());
	return [gdriver, chunkStore];
}

describe('GDriver', function() {
	it('can upload a file, create folder, get file, delete both', async function() {
		this.timeout(20000);

		const [gdriver, chunkStore] = await doSetup();

		const tempFname = path.join(os.tmpdir(), 'terastash-gdrive-tests-' + String(Math.random()));
		const fileLength = utils.randInt(1 * 1024, 5 * 1024);
		const buf = crypto.pseudoRandomBytes(fileLength);
		A.eq(buf.length, fileLength);
		await fs.writeFileAsync(tempFname, buf);

		let _ = await Promise.all([
			gdriver.createFile(
				"test-file", {parents: chunkStore.parents}, fs.createReadStream(tempFname)
			),
			gdriver.createFolder(
				"test-folder", {parents: chunkStore.parents}
			)
		]);
		const createFileResponse = _[0];
		const createFolderResponse = _[1];
		A.eq(typeof createFileResponse.id, "string");
		A.eq(typeof createFolderResponse.id, "string");

		_ = await Promise.all([
			gdriver.getMetadata(createFileResponse.id),
			gdriver.getData(createFileResponse.id),
			gdriver.getData(createFileResponse.id, [0, 100])
		]);

		const getMetadataResponse = _[0];
		A.eq(getMetadataResponse.md5Checksum, createFileResponse.md5Checksum);

		// Make sure getData gives us bytes that match what we uploaded
		const dataStream = _[1][0];
		const data = await utils.readableToBuffer(dataStream);
		A.eq(data.length, buf.length);
		const dataDigest = crypto.createHash("md5").update(data).digest("hex");
		A.eq(dataDigest, createFileResponse.md5Checksum);

		const partialDataStream = _[2][0];
		A.neq(dataStream, partialDataStream);
		const partialData = await utils.readableToBuffer(partialDataStream);
		assert.deepStrictEqual(partialData, buf.slice(0, 100));

		await Promise.all([
			gdriver.deleteFile(createFileResponse.id),
			gdriver.deleteFile(createFolderResponse.id)
		]);

		// Deleting a file that doesn't exist throws an error
		let caught;
		try {
			await gdriver.deleteFile(createFileResponse.id);
		} catch(err) {
			caught = err;
		}
		A(caught instanceof Error, `deleteFile on nonexistent file did not throw Error; caught=${caught}`);
	});

	it('getData does not hang forever when Google Drive reports 404 for a fileId', async function() {
		this.timeout(6000);

		const [gdriver, _chunkStore] = await doSetup();

		let caught;
		try {
			await gdriver.getData("bogus_file_id");
		} catch(e) {
			caught = e;
		}
		A(caught instanceof gdrive.DownloadError);
	});

	it('readChunks does not hang forever when Google Drive reports 404 for a fileId', async function() {
		this.timeout(6000);

		const [gdriver, _chunkStore] = await doSetup();

		const wantedChunks = [{
			"idx": 0,
			"file_id": "bogus_file_id",
			"crc32c": Buffer.from("abcd"),
			"size": 0
		}];
		const wantedRanges = [[0, 1]];
		const checkWholeChunkCRC32C = false;

		const cipherStream = gdrive.readChunks(gdriver, wantedChunks, wantedRanges, checkWholeChunkCRC32C);
		let doneCb;
		let result;
		const done = new Promise(function(resolve) { doneCb = resolve; });
		cipherStream.once('end',   (_) =>  { result = ["end"];       doneCb(); });
		cipherStream.once('error', (ev) => { result = ["error", ev]; doneCb(); });
		await done;
		A.eq(result[0], "error");
		A(result[1] instanceof gdrive.DownloadError, result[1]);
	});
});
