"use strong";
"use strict";

const google = require('googleapis');
const Promise = require('bluebird');
const chopshop = require('chopshop');
const A = require('ayy');
const T = require('notmytype');
const Combine = require('combine-streams');
const OAuth2 = google.auth.OAuth2;
const utils = require('../utils');
const inspect = require('util').inspect;

const getAllCredentials = utils.makeConfigFileInitializer(
	"google-tokens.json", {
		credentials: {},
		_comment: "Access tokens expire quickly; refresh tokens never expire unless revoked."
	}
);

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
		this.clientId = clientId;
		this.clientSecret = clientSecret;
		const redirectUrl = 'urn:ietf:wg:oauth:2.0:oob';
		this._oauth2Client = new OAuth2(clientId, clientSecret, redirectUrl);

		// Work around https://github.com/google/google-api-nodejs-client/issues/260
		// by patching getRequestMetadata with something that never returns an
		// auth request to googleapis/lib/apirequest.js:createAPIRequest
		//
		// If we don't patch this, the buggy googleapis/google-auth-library interaction
		// will hang terastash forever when we try to upload a file when our access token
		// is expired.  (createAPIRequest decides to pipe the stream into the auth request
		// instead of the subsequent request.)
		//
		// We could always refresh the access token ourselves, but we prefer to also
		// patch the buggy code to prevent bugs from compounding.
		this._oauth2Client.getRequestMetadata = function(optUri, metadataCb) {
			const thisCreds = this.credentials;

			if (!thisCreds.access_token && !thisCreds.refresh_token) {
				return metadataCb(new Error('No access or refresh token is set.'), null);
			}

			// if no expiry time, assume it's not expired
			const expiryDate = thisCreds.expiry_date;
			const isTokenExpired = expiryDate ? expiryDate <= (new Date()).getTime() : false;

			if (thisCreds.access_token && !isTokenExpired) {
				thisCreds.token_type = thisCreds.token_type || 'Bearer';
				const headers = {'Authorization': thisCreds.token_type + ' ' + thisCreds.access_token};
				return metadataCb(null, headers, null);
			} else {
				return metadataCb(new Error('Access token is expired.'), null);
			}
		};

		this._drive = google.drive({version: 'v2', auth: this._oauth2Client});
	}

	getAuthUrl() {
		const scopes = ['https://www.googleapis.com/auth/drive'];
		const url = this._oauth2Client.generateAuthUrl({
			"access_type": 'offline', // 'online' (default) or 'offline' (gets refresh_token)
			"scope": scopes
		});
		return url;
	}

	getCredentials() {
		return this._oauth2Client.credentials;
	}

	/**
	 * Hit Google to get an access and refresh token based on `authCode`
	 * and set the tokens on the `oauth2Client` object.
	 *
	 * Returns a Promise that resolves with null after the credentials are
	 * saved.
	 */
	importAuthCode(authCode) {
		T(authCode, T.string);
		return new Promise(function(resolve, reject) {
			this._oauth2Client.getToken(authCode, function(err, tokens) {
				if(err) {
					reject(err);
				} else {
					this._oauth2Client.setCredentials(tokens);
					resolve(this.saveCredentials());
				}
			}.bind(this));
		}.bind(this));
	}

	*loadCredentials() {
		const config = yield getAllCredentials();
		const credentials = config.credentials[this.clientId];
		if(credentials) {
			this._oauth2Client.setCredentials(credentials);
		}
	}

	*saveCredentials() {
		const config = yield getAllCredentials();
		config.credentials[this.clientId] = this._oauth2Client.credentials;
		//console.log("Saving credentials", this._oauth2Client.credentials);
		return utils.writeObjectToConfigFile("google-tokens.json", config);
	}

	refreshAccessToken() {
		return new Promise(function(resolve, reject) {
			this._oauth2Client.refreshAccessToken(function(err) {
				if(err) {
					reject(err);
				} else {
					resolve(null);
				}
			});
		}.bind(this));
	}

	*_maybeRefreshAndSaveToken() {
		// Access tokens last for 60 minutes; make sure we have at least 50 minutes
		// left on the clock, in case our upload takes a while.
		const minMinutes = 50;
		if(!(this._oauth2Client.credentials.expiry_date >= Date.now() + (minMinutes * 60 * 1000))) {
			//console.log("Refreshing access token...");
			yield this.refreshAccessToken();
			A.gte(this._oauth2Client.credentials.expiry_date, Date.now() + (minMinutes * 60 * 1000));
			yield this.saveCredentials();
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
	*createFile(name, opts, stream, requestCb) {
		T(
			name, T.string,
			opts, T.shape({
				parents: T.optional(T.list(T.string)),
				mimeType: T.optional(T.string)
			}),
			stream, T.maybe(T.object),
			requestCb, T.optional(T.object)
		);

		yield this._maybeRefreshAndSaveToken();

		const parents = (opts.parents || utils.emptyFrozenArray).concat().sort();
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
				mimeType: mimeType
			}
		};
		let hasher;
		if(stream !== null) {
			hasher = utils.streamHasher(stream, 'md5');
			insertOpts.media = {
				mimeType: mimeType,
				body: hasher.stream
			};
		}

		return new Promise(function(resolve, reject) {
			const requestObj = this._drive.files.insert(insertOpts, function(err, obj) {
				if(err) {
					reject(err);
				} else {
					resolve(obj);
				}
			});
			if(requestCb) {
				requestCb(requestObj);
			}
		}.bind(this)).then(function(obj) {
			T(obj, T.object);
			if(obj.kind !== "drive#file") {
				throw new UploadError(`Expected Google Drive to create an` +
					` object with kind='drive#file' but was ${inspect(obj.kind)}`
				);
			}
			if(typeof obj.id !== "string") {
				throw new UploadError(`Expected Google Drive to create a` +
					` file with id=(string) but was id=${inspect(obj.id)}`
				);
			}
			if(stream && obj.fileSize !== String(hasher.length)) {
				throw new UploadError(`Expected Google Drive to create a` +
					` file with fileSize=${inspect(String(hasher.length))} but was ${inspect(obj.fileSize)}`
				);
			}
			if(parents.length !== 0) {
				const parentsInDrive = obj.parents.map(idProp).sort();
				if(!utils.sameArrayValues(parentsInDrive, parents)) {
					throw new UploadError(`Expected Google Drive to create a file` +
						` with parents=${inspect(parents)} but was ${inspect(parentsInDrive)}.\n` +
						`Make sure you specified the correct folder IDs.`
					);
				}
			}
			if(stream) {
				const expectedHexDigest = hasher.hash.digest('hex');
				if(obj.md5Checksum !== expectedHexDigest) {
					throw new UploadError(`Expected Google Drive to create a` +
						` file with md5Checksum=${inspect(expectedHexDigest)}` +
						` but was ${inspect(obj.md5Checksum)}`
					);
				}
			}
			// obj.mimeType may not match what we wanted, so don't check it
			return obj;
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
	*deleteFile(fileId) {
		T(fileId, T.string);
		yield this._maybeRefreshAndSaveToken();
		return new Promise(function(resolve, reject) {
			this._drive.files.delete({fileId}, function(err, obj) {
				if(err) {
					reject(err);
				} else {
					resolve(obj);
				}
			});
		}.bind(this));
	}

	*getMetadata(fileId) {
		T(fileId, T.string);
		yield this._maybeRefreshAndSaveToken();
		return new Promise(function(resolve, reject) {
			this._drive.files.get(
				{
					fileId,
					updateViewedDate: false
				},
				function(err, obj) {
					if(err) {
						reject(err);
					} else {
						resolve(obj);
					}
				}
			);
		}.bind(this));
	}

	_getHeaders() {
		const credentials = this._oauth2Client.credentials;
		if(!credentials) {
			throw new Error("Lack credentials");
		}
		if(!credentials.token_type) {
			throw new Error("Credentials lack token_type");
		}
		if(!credentials.access_token) {
			throw new Error("Credentials lack access_token");
		}
		return {"Authorization": `${credentials.token_type} ${credentials.access_token}`};
	}

	/**
	 * fileId is the file's fileId on Google Drive (not the filename)
	 * range is an optional [start, end] where start is inclusive and end is exclusive
	 *
	 * Returns a Promise that is resolved with [stream, http response].
	 * You must read from the stream, not the http response.
	 * For full (non-Range) requests, the crc32c checksum from Google is verified.
	 */
	*getData(fileId, range) {
		T(fileId, T.string, range, T.optional(T.tuple([T.number, T.number])));
		if(range) {
			A(Number.isInteger(range[0]), range[0], "must be an integer");
			A(Number.isInteger(range[1]), range[1], "must be an integer");
			A.gte(range[0], 0);
			A.gte(range[1], range[0], "end must be >= start in range [start, end]");
		}
		yield this._maybeRefreshAndSaveToken();
		const reqHeaders = this._getHeaders();
		if(range) {
			reqHeaders["Range"] = `bytes=${range[0]}-${range[1] - 1}`;
		}
		const res = yield utils.makeHttpsRequest({
			method: "GET",
			host: "www.googleapis.com",
			path: `/drive/v2/files/${fileId}?alt=media`,
			headers: reqHeaders
		});
		if((!range && res.statusCode === 200) || (range && res.statusCode === 206)) {
			if(res.statusCode === 206) {
				const contentRange = res.headers['content-range'];
				const expectedContentRange = `bytes ${range[0]}-${range[1] - 1}/`;
				if(!contentRange.startsWith(expectedContentRange)) {
					throw new Error(`Expected 'content-range' header to start with`
						` ${expectedContentRange} but was ${contentRange}`
					);
				}
			}
			const hasher = utils.streamHasher(res, 'crc32c');
			const googHash = res.headers['x-goog-hash'];
			let googCRC;
			// x-goog-hash header should always be present on 200 responses;
			// also on 206 responses if you requested all of the bytes.
			if(res.statusCode === 200 && !googHash) {
				throw new Error("x-goog-hash header was missing on a 200 response");
			}
			if(googHash) {
				A(googHash.startsWith("crc32c="), googHash);
				googCRC = new Buffer(googHash.replace("crc32c=", ""), "base64");
			}
			hasher.stream.once('end', function() {
				const computedCRC = hasher.hash.digest();
				if(googCRC && !computedCRC.equals(googCRC)) {
					hasher.stream.emit('error', new Error(
						`CRC32c check failed on fileId=${inspect(fileId)}:` +
						` expected ${googCRC.toString("hex")},` +
						` got ${computedCRC.toString("hex")}`
					));
				}
			});
			return [hasher.stream, res];
		} else {
			const body = yield utils.streamToBuffer(res);
			if((res.headers['content-type'] || "").toLowerCase() === 'application/json; charset=utf-8') {
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

GDriver.prototype.createFile = Promise.coroutine(GDriver.prototype.createFile);
GDriver.prototype.loadCredentials = Promise.coroutine(GDriver.prototype.loadCredentials);
GDriver.prototype.saveCredentials = Promise.coroutine(GDriver.prototype.saveCredentials);
GDriver.prototype.deleteFile = Promise.coroutine(GDriver.prototype.deleteFile);
GDriver.prototype.getMetadata = Promise.coroutine(GDriver.prototype.getMetadata);
GDriver.prototype.getData = Promise.coroutine(GDriver.prototype.getData);
GDriver.prototype._maybeRefreshAndSaveToken = Promise.coroutine(GDriver.prototype._maybeRefreshAndSaveToken);


const writeChunks = Promise.coroutine(function*(gdriver, parents, cipherStream, chunkSize) {
	T(gdriver, GDriver, parents, T.list(T.string), cipherStream, T.shape({pipe: T.function}), chunkSize, T.number);

	// Chunk size must be a multiple of an AES block, for implementation convenience.
	A.eq(chunkSize % 128/8, 0);

	let totalSize = 0;
	let idx = 0;
	const chunkInfo = [];

	for(const chunkStream of chopshop.chunk(cipherStream, chunkSize)) {
		const fname = utils.makeChunkFilename();
		const crc32Hasher = utils.streamHasher(chunkStream, 'crc32c');
		const response = yield gdriver.createFile(fname, {parents}, crc32Hasher.stream);
		// We can trust the md5Checksum in response; createFile checked it for us
		const md5Digest = new Buffer(response['md5Checksum'], 'hex');
		chunkInfo.push({
			idx,
			file_id: response.id,
			crc32c: crc32Hasher.hash.digest(),
			md5: md5Digest,
			size: crc32Hasher.length
		});
		totalSize += crc32Hasher.length;
		idx += 1;
	}
	return [totalSize, chunkInfo];
});

class BadChunk extends Error {
	get name() {
		return this.constructor.name;
	}
}

/**
 * Returns a readable stream by decrypting and concatenating the chunks.
 */
function readChunks(gdriver, chunks) {
	T(gdriver, GDriver, chunks, utils.ChunksType);

	const cipherStream = new Combine();
	// We don't return this Promise; we return the stream and
	// the coroutine does the work of writing to the stream.
	Promise.coroutine(function*() {
		for(const chunk of chunks) {
			const _ = yield gdriver.getData(chunk.file_id);
			const chunkStream = _[0];
			const res = _[1];

			const googHash = res.headers['x-goog-hash'];
			T(googHash, T.string);
			A(googHash.startsWith("crc32c="), googHash);
			const googCRC = new Buffer(googHash.replace("crc32c=", ""), "base64");
			if(!chunk.crc32c.equals(googCRC)) {
				throw new Error(
					`For chunk with file_id=${inspect(chunk.file_id)} (chunk #${chunk.idx} for file),\n` +
					`expected Google to send crc32c\n` +
					`${chunk.crc32c.toString('hex')} but got\n` +
					`${googHash.toString('hex')}`);
			}

			cipherStream.append(chunkStream);
			yield new Promise(function(resolve) {
				chunkStream.once('end', resolve);
			});
		}
		cipherStream.append(null);
	})().catch(function(err) {
		cipherStream.emit('error', err);
	});
	return cipherStream;
}

module.exports = {GDriver, writeChunks, readChunks};
