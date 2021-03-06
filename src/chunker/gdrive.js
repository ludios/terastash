"use strict";

const A                 = require('ayy');
const T                 = require('notmytype');
const Combine           = require('combine-streams');
const crypto            = require('crypto');
const { google }        = require('googleapis');
const utils             = require('../utils');
const OutputContextType = utils.OutputContextType;
const retry             = require('../retry');
const inspect           = require('util').inspect;
const chalk             = require('chalk');
const path              = require('path');
const fs                = require('../fs-promisified');
const basedir           = require('xdg').basedir;
const compile_require   = require('../compile_require');
const sse4_crc32        = compile_require('sse4_crc32');


function getTokenFiles() {
	let tokenFiles = null;
	try {
		// google-tokens/ for multiple accounts
		const dir = basedir.configPath(path.join("terastash", "google-tokens"));
		tokenFiles = fs.readdirSync(dir);
	} catch(e) {}
	return tokenFiles;
}

function getAccounts() {
	const tokenFiles = getTokenFiles();
	return tokenFiles.map((tokenFile) => tokenFile.replace(/\.json$/, ""));
}

function pickRandomAccount() {
	const accounts = getAccounts();
	const account  = accounts[Math.floor(Math.random() * accounts.length)];
	return account;
}

async function getAllCredentialsForAccount(account) {
	T(account, T.string);
	const tokenFile = basedir.configPath(path.join("terastash", "google-tokens", `${account}.json`));
	const buf       = await fs.readFileAsync(tokenFile);
	return JSON.parse(buf);
}

function getRandomServiceAccountFile() {
	const dir = basedir.configPath(path.join("terastash", "service-accounts"));
	const serviceAccounts = fs.readdirSync(dir);
	const accountFile = serviceAccounts[Math.floor(Math.random() * serviceAccounts.length)];
	return path.join(dir, accountFile);
}

function crc32cHexDigest(s) {
	const digest = sse4_crc32.calculate(s);
	const buf = Buffer.allocUnsafe(4);
	buf.writeUIntBE(digest, 0, 4);
	return buf.toString("hex");
}

// Used to prevent real emails from showing up on the grafana dashboard
function obfuscateAccountName(account) {
	let prefix = "G-";
	if (account.endsWith(".gserviceaccount.com")) {
		prefix = "S-";
	}
	return `${prefix}${crc32cHexDigest(account).toUpperCase()}`;
}

const idProp = utils.prop('id');

class UploadError extends Error {
	get name() {
		return this.constructor.name;
	}
}

class DownloadError extends Error {
	get name() {
		return this.constructor.name;
	}
}

class GDriver {
	constructor(clientId, clientSecret) {
		T(clientId, T.string, clientSecret, T.string);
		this.clientId      = clientId;
		this.clientSecret  = clientSecret;
		this._account      = null;
		this._auth         = null;
		this._drive        = null;
	}

	getAuthUrl() {
		const url = this._oauth2Client.generateAuthUrl({
			"access_type": "offline", // "online" (default) or "offline" (gets refresh_token)
			"scope": ["https://www.googleapis.com/auth/drive"]
		});
		return url;
	}

	getCredentials() {
		return this._oauth2Client.credentials;
	}

	saveCredentials(account) {
		const config = {credentials: {[this.clientId]: this._oauth2Client.credentials}};
		return utils.writeObjectToConfigFile(`google-tokens/${account}.json`, config);
	}

	/**
	 * Hit Google to get an access and refresh token based on `authCode`
	 * and set the tokens on the `oauth2Client` object.
	 *
	 * Returns a Promise that resolves with null after the credentials are
	 * saved.
	 */
	importAuthCode(authCode, account) {
		T(authCode, T.string, account, T.string);
		return new Promise(function(resolve, reject) {
			this._oauth2Client.getToken(authCode, function(err, tokens) {
				if (err) {
					reject(err);
				} else {
					this._oauth2Client.setCredentials(tokens);
					resolve(this.saveCredentials(account));
					resolve(null);
				}
			}.bind(this));
		}.bind(this));
	}

	async loadOAuth2Credentials(account) {
		T(account, T.string);
		const config = await getAllCredentialsForAccount(account);
		const credentials = config.credentials[this.clientId];
		const redirectUrl = 'urn:ietf:wg:oauth:2.0:oob';
		this._auth = new google.auth.OAuth2(this.clientId, this.clientSecret, redirectUrl);
		this._auth.setCredentials(credentials);
		this._account = account;
		this._drive = google.drive({version: 'v2', auth: this._auth});
	}

	async createTeamDrive(managerEmail, contentManagers, name) {
		T(managerEmail, T.string, contentManagers, T.list(T.string), name, T.string);
		await this.loadOAuth2Credentials(managerEmail);
		this._drivev3   = google.drive({version: 'v3', auth: this._auth});

		const requestId = crypto.randomBytes(128/8).toString('hex');
		const params1   = {requestId: requestId, resource: {name: name}};
		const reply1    = await this._drivev3.drives.create(params1);
		const data      = reply1.data;
		A.eq(data.kind, "drive#drive");
		A.eq(data.name, name);
		T(data.id, T.string);

		for (const email of contentManagers) {
			await this.addContentManager(data.id, email);
		}

		return data.id;
	}

	async addContentManager(teamDriveId, emailAddress) {
		T(teamDriveId, T.string, emailAddress, T.string);
		const body     = {kind: "drive#permission", type: "user", emailAddress: emailAddress, role: "fileOrganizer"};
		const params   = {fileId: teamDriveId, supportsAllDrives: true, sendNotificationEmail: false, requestBody: body};
		const reply    = await this._drivev3.permissions.create(params);
		A.eq(reply.status, 200);
	}

	async loadServiceAccountCredentials() {
		this._auth = await google.auth.getClient({
			keyFile: getRandomServiceAccountFile(),
			scopes: "https://www.googleapis.com/auth/drive",
		});
		this._account = "teamdrive";
		this._drive = google.drive({version: 'v2', auth: this._auth});
	}

	refreshAccessToken() {
		return new Promise(function(resolve, reject) {
			this._oauth2Client.refreshAccessToken(function(err) {
				if (err) {
					reject(err);
				} else {
					resolve(null);
				}
			});
		}.bind(this));
	}

	// Only needed for non-service-account operations
	async _maybeRefreshAndSaveToken() {
		// Typically, another process is responsible for updating the tokens and
		// copying them to all the machines that might need them.
		if (!Number(process.env.TERASTASH_REFRESH_GOOGLE_TOKENS)) {
			return;
		}
		// Access tokens last for 60 minutes; make sure we have at least 50 minutes
		// left on the clock, in case our upload takes a while.
		const minMinutes = 50;
		if (!(this._oauth2Client.credentials.expiry_date >= Date.now() + (minMinutes * 60 * 1000))) {
			//console.log("Refreshing access token...");
			await this.refreshAccessToken();
			A.gte(this._oauth2Client.credentials.expiry_date, Date.now() + (minMinutes * 60 * 1000));
			//await this.saveCredentials();
		}
	}

	/**
	 * Returns a Promise that is resolved with the response from Google,
	 * mostly importantly containing an "id" property with the file ID that
	 * Google has assigned to it.
	 *
	 * Note: Google Drive may replace your opts.mimeType with their own
	 * mimeType after sniffing the bytes in your file.  This is not documented
	 * in their API docs, and there doesn't appear to be a way to turn it off.
	 */
	async createFile(name, opts, stream, requestCb) {
		T(
			name, T.string,
			opts, T.shape({
				parents: T.optional(T.list(T.string)),
				mimeType: T.optional(T.string)
			}),
			stream, T.maybe(T.object),
			requestCb, T.optional(T.object)
		);

		await this.loadServiceAccountCredentials();

		const parents = (opts.parents || []).concat().sort();
		const mimeType = opts.mimeType || "application/octet-stream";

		const insertOpts = {
			resource: {
				title: name,
				parents: parents.map(function(parentId) {
					return {
						"kind": "drive#fileLink",
						"id": parentId
					};
				}),
				mimeType: mimeType,
			},
			supportsAllDrives: true
		};
		let hasher;
		if (stream !== null) {
			hasher = utils.streamHasher(stream, 'md5');
			insertOpts.media = {
				mimeType: mimeType,
				body: hasher.stream
			};
		}

		const account = this._account;
		return new Promise(function(resolve, reject) {
			const requestObj = this._drive.files.insert(insertOpts, function(err, obj) {
				if (err) {
					if (err.code === 404 &&
					err.errors instanceof Array &&
					err.errors.length >= 1 &&
					err.errors[0].reason === 'notFound') {
						reject(new UploadError(`googleapis.com returned:\n${err.message}\n\n` +
							`Make sure that the Google Drive folder you are uploading into exists ` +
							`("parents" in config), and that you are using credentials for ` +
							`the correct Google account.`));
					} else {
						reject(err);
					}
				} else {
					resolve(obj);
				}
			});
			if (requestCb) {
				requestCb(requestObj);
			}
		}.bind(this)).then(function(reply) {
			T(reply, T.object);
			const data = reply.data;
			if (data.kind !== "drive#file") {
				throw new UploadError(`Expected Google Drive to create an` +
					` object with kind='drive#file' but was ${inspect(data.kind)}`
				);
			}
			if (typeof data.id !== "string") {
				throw new UploadError(`Expected Google Drive to create a` +
					` file with id=(string) but was id=${inspect(data.id)}`
				);
			}
			if (stream && data.fileSize !== String(hasher.length)) {
				throw new UploadError(`Expected Google Drive to create a` +
					` file with fileSize=${inspect(String(hasher.length))} but was ${inspect(data.fileSize)}`
				);
			}
			if (parents.length !== 0) {
				const parentsInDrive = data.parents.map(idProp).sort();
				if (!utils.sameArrayValues(parentsInDrive, parents)) {
					throw new UploadError(`Expected Google Drive to create a file` +
						` with parents=${inspect(parents)} but was ${inspect(parentsInDrive)}.\n` +
						`Make sure you specified the correct folder IDs.`
					);
				}
			}
			if (stream) {
				const expectedHexDigest = hasher.hash.digest('hex');
				if (data.md5Checksum !== expectedHexDigest) {
					throw new UploadError(`Expected Google Drive to create a` +
						` file with md5Checksum=${inspect(expectedHexDigest)}` +
						` but was ${inspect(data.md5Checksum)}`
					);
				}

				utils.statsdCounterIncrement(
					"terastash_bytes_uploaded", hasher.length,
					[`account:${obfuscateAccountName(account)}`]);
			}
			// data.mimeType may not match what we wanted, so don't check it
			return data;
		});
	}

	createFolder(name, opts) {
		T(
			name, T.string,
			opts, T.shape({
				parents: T.optional(T.list(T.string)),
				mimeType: T.optional(T.string)
			})
		);
		opts = utils.clone(opts);
		opts.mimeType = "application/vnd.google-apps.folder";
		return this.createFile(name, opts, null);
	}

	/**
	 * Delete a file or folder by ID
	 */
	async deleteFile(fileId) {
		T(fileId, T.string);
		await this._maybeRefreshAndSaveToken();
		return new Promise(function(resolve, reject) {
			this._drive.files.delete({fileId, supportsAllDrives: true}, function(err, obj) {
				if (err) {
					reject(err);
				} else {
					resolve(obj);
				}
			});
		}.bind(this));
	}

	async getMetadata(fileId) {
		T(fileId, T.string);
		return new Promise(function(resolve, reject) {
			this._drive.files.get(
				{
					fileId,
					updateViewedDate: false,
					supportsAllDrives: true
				},
				function(err, reply) {
					if (err) {
						reject(err);
					} else {
						resolve(reply.data);
					}
				}
			);
		}.bind(this));
	}

	/**
	 * fileId is the file's fileId on Google Drive (not the filename)
	 * range is an optional [start, end] where start is inclusive and end is exclusive
	 *
	 * Returns a Promise that is resolved with [stream, http response].
	 * You must read from the stream, not the http response.
	 * For full (non-Range) requests, the crc32c checksum from Google is verified.
	 */
	async getData(fileId, range, checkCRC32CifReceived=true) {
		T(fileId, T.string, range, T.optional(utils.RangeType), checkCRC32CifReceived, T.boolean);
		if (range) {
			utils.checkRange(range);
		}
		const reqHeaders = await this._auth.getRequestHeaders();
		if (range) {
			reqHeaders["Range"] = `bytes=${range[0]}-${range[1] - 1}`;
		}
		const res = await utils.makeHttpsRequest({
			method: "GET",
			host: "www.googleapis.com",
			path: `/drive/v2/files/${fileId}?alt=media`,
			headers: reqHeaders
		});
		if ((!range && res.statusCode === 200) || (range && res.statusCode === 206)) {
			if (res.statusCode === 206) {
				const contentRange = res.headers['content-range'];
				const expectedContentRange = `bytes ${range[0]}-${range[1] - 1}/`;
				if (!contentRange.startsWith(expectedContentRange)) {
					throw new Error(`Expected 'content-range' header to start with` +
						` ${expectedContentRange} but was ${contentRange}`
					);
				}
			}
			const googHash = res.headers['x-goog-hash'];
			// x-goog-hash header should always be present on 200 responses;
			// also on 206 responses if you requested all of the bytes.
			if (res.statusCode === 200 && !googHash) {
				throw new Error("x-goog-hash header was missing on a 200 response");
			}
			let hasher;
			if (googHash && checkCRC32CifReceived) {
				hasher = utils.streamHasher(res, 'crc32c');
				A(googHash.startsWith("crc32c="), googHash);
				const googCRC = Buffer.from(googHash.replace("crc32c=", ""), "base64");
				hasher.stream.once('end', function getData$hasher$end() {
					const computedCRC = hasher.hash.digest();
					if (!computedCRC.equals(googCRC)) {
						hasher.stream.emit('error', new Error(
							`CRC32c check failed on fileId=${inspect(fileId)}:` +
							` expected ${googCRC.toString("hex")},` +
							` got ${computedCRC.toString("hex")}`
						));
					}
				});
			}
			return [(hasher ? hasher.stream : res), res];
		} else {
			let body = await utils.readableToBuffer(res);
			if ((res.headers['content-type'] || "").toLowerCase() === 'application/json; charset=utf-8') {
				try {
					body = JSON.parse(body);
				} catch(e) {
					// Leave body as-is
				}
			}
			throw new DownloadError(
				`Got response with status ${res.statusCode} and body ${inspect(body)}`
			);
		}
	}
}


async function writeChunks(outCtx, gdriver, getParents, getChunkStream) {
	T(outCtx, OutputContextType, gdriver, GDriver, getParents, T.function, getChunkStream, T.function);

	let totalSize = 0;
	let idx = 0;
	const chunkInfo = [];

	while (true) {
		const decayer = new retry.Decayer(5 * 1000, 1.5, 3600 * 1000);
		let lastChunkAgain = false;
		let crc32Hasher;
		const response = await retry.retryFunction(async function writeChunks$retry() {
			// Make a new filename each time, in case server reports error
			// when it actually succeeded.
			const fname = utils.makeChunkFilename();
			const chunkStream = await getChunkStream(lastChunkAgain);
			if (chunkStream === null) {
				return null;
			}
			crc32Hasher = utils.streamHasher(chunkStream, 'crc32c');
			if (Math.random() < Number(process.env.TERASTASH_UPLOAD_FAIL_RATIO)) {
				throw new Error("Forcing a failure for testing (TERASTASH_UPLOAD_FAIL_RATIO is set)");
			}
			const parents = await getParents();
			T(parents, T.list(T.string));
			return gdriver.createFile(fname, {parents}, crc32Hasher.stream);
		}, function writeChunks$errorHandler(e, triesLeft) {
			lastChunkAgain = true;
			if (outCtx.mode !== 'quiet') {
				console.error(`Error while uploading chunk ${idx}:\n`);
				console.error(e.stack);
				console.error(`\n${utils.pluralize(triesLeft, 'try', 'tries')} left; ` +
					`trying again in ${decayer.getNextDelay()/1000} seconds...`);
			}
		}, 50, decayer);
		if (response === null) {
			break;
		}
		// We can trust the md5Checksum in response; createFile checked it for us
		const md5Digest = Buffer.from(response['md5Checksum'], 'hex');
		chunkInfo.push({
			idx:     idx,
			file_id: response.id,
			crc32c:  crc32Hasher.hash.digest(),
			md5:     md5Digest,
			size:    crc32Hasher.length,
			account: gdriver._account
		});
		totalSize += crc32Hasher.length;
		idx += 1;
	}
	return [totalSize, chunkInfo];
}

class BadChunk extends Error {
	get name() {
		return this.constructor.name;
	}
}

/**
 * Returns a readable stream of concatenated chunks.
 */
function readChunks(gdriver, chunks, ranges, checkWholeChunkCRC32C) {
	T(gdriver, GDriver, chunks, utils.ChunksType, ranges, utils.RangesType, checkWholeChunkCRC32C, T.boolean);
	A.eq(chunks.length, ranges.length);

	const cipherStream = new Combine();
	let currentChunkStream = null;
	let destroyed = false;
	// We don't return this Promise; we return the stream and
	// the coroutine does the work of writing to the stream.
	(async function readChunks$coro() {
		for (const [chunk, range] of utils.zip(chunks, ranges)) {
			if (destroyed) {
				return;
			}

			// Read using the account that was used to upload the file, because
			// Google Drive apparently does not always apply the folder's sharing
			// options to every file in a folder, leaving older files readable with
			// (likely) the set of accounts that the folder was shared with
			// at the time of file upload.
			//
			// We try three times because sometimes the Google Drive backend returns
			// spurious transient 404s.
			let accountsToTry;
			if (chunk.account && chunk.account !== "teamdrive") {
				accountsToTry = [chunk.account, chunk.account, chunk.account];
			} else {
				const accounts = getAccounts();
				accountsToTry = [].concat(accounts, accounts, accounts);
			}

			let getDataError, chunkStream, res;
			for (const account of accountsToTry) {
				// Use one of our regular accounts to read, because our non-Team Drive files can't be read
				// with the service accounts.
				await gdriver.loadOAuth2Credentials(account);
				try {
					[chunkStream, res] = await gdriver.getData(chunk.file_id, range, checkWholeChunkCRC32C);
					break;
				} catch(e) {
					if (!(e instanceof DownloadError)) {
						throw e;
					}
					getDataError = e;
				}
			}
			if (!chunkStream) {
				throw getDataError;
			}
			currentChunkStream = chunkStream;
			// Note: even when we're not checking whole-chunk CRC32C, we're still
			// making sure Google is sending us the correct CRC32C for a file.
			// Though we don't have the opportunity to do this when we're getting a
			// range that is not the whole file.
			const googHash = res.headers['x-goog-hash'];
			if (googHash === undefined) {
				if (range[1] - range[0] === chunk.size) {
					throw new Error(`Downloading a whole file, but did not receive ` +
						`x-goog-hash in headers:\n${inspect(res.headers)}`);
				}
			} else {
				T(googHash, T.string);
				A(googHash.startsWith("crc32c="), googHash);
				const googCRC = Buffer.from(googHash.replace("crc32c=", ""), "base64");
				if (!chunk.crc32c.equals(googCRC)) {
					throw new BadChunk(
						`For chunk with file_id=${inspect(chunk.file_id)} (chunk #${chunk.idx} for file),\n` +
						`expected Google to send crc32c\n` +
						`${chunk.crc32c.toString('hex')} but got\n` +
						`${googHash.toString('hex')}`);
				}
			}
			cipherStream.append(chunkStream);
			await new Promise(function(resolve) {
				chunkStream.once('end', resolve);
			});
		}
		cipherStream.append(null);
	})().catch(function(err) {
		cipherStream.emit('error', err);
	});
	cipherStream.destroy = function cipherStream$destroy() {
		destroyed = true;
		currentChunkStream.destroy();
	};
	return cipherStream;
}

/**
 * Deletes chunks
 */
async function deleteChunks(gdriver, chunks) {
	T(gdriver, GDriver, chunks, utils.ChunksType);
	for (const chunk of chunks) {
		try {
			const account = chunk.account;
			if (account && account !== "teamdrive") {
				T(account, T.string);
				if (account.indexOf("/") != -1) {
					throw new Error(`Illegal character in account=${inspect(account)}` +
						` in chunks=${inspect(chunks)}`);
				}
				await gdriver.loadOAuth2Credentials(account);
				await gdriver.deleteFile(chunk.file_id);
			} else {
				// Even if we use Drive permissions to share files with multiple accounts,
				// Google lets us delete a file only from the account that uploaded it.
				//
				// Older versions of terastash did not record which account was used to
				// upload each chunk.  If we did not record who owns a chunk, try every
				// account.
				for (const account_ of getAccounts()) {
					try {
						await gdriver.loadOAuth2Credentials(account_);
						await gdriver.deleteFile(chunk.file_id);
						break;
					} catch(_) {
					}
				}
			}
		} catch(err) {
			console.error(chalk.red(
				`Failed to delete chunk with file_id=${inspect(chunk.file_id)}` +
				` (chunk #${chunk.idx} for file)`));
			console.error(chalk.red(err.stack));
		}
	}
}

module.exports = {GDriver, writeChunks, readChunks, deleteChunks, pickRandomAccount, DownloadError, obfuscateAccountName};
