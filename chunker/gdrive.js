"use strong";
"use strict";

const google = require('googleapis');
const Promise = require('bluebird');
const A = require('ayy');
const T = require('notmytype');
const OAuth2 = google.auth.OAuth2;
const utils = require('../utils');
const inspect = require('util').inspect;
const crypto = require('crypto');
const PassThrough = require('stream').PassThrough;

const REDIRECT_URL = 'urn:ietf:wg:oauth:2.0:oob';

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
		this._oauth2Client = new OAuth2(clientId, clientSecret, REDIRECT_URL);

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

		let passthrough;
		let length = 0;
		let md5;
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
		if(stream !== null) {
			md5 = crypto.createHash('md5');
			passthrough = new PassThrough();
			stream.pipe(passthrough);
			passthrough.on('data', function(data) {
				length += data.length;
				md5.update(data);
			});

			insertOpts.media = {
				mimeType: mimeType,
				body: passthrough
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
			if(stream && obj.fileSize !== String(length)) {
				throw new UploadError(`Expected Google Drive to create a` +
					` file with fileSize=${inspect(String(length))} but was ${inspect(obj.fileSize)}`
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
				const expectedHexDigest = md5.digest('hex');
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

	*getData(fileId) {
		T(fileId, T.string);
		yield this._maybeRefreshAndSaveToken();
		return utils.makeHttpsRequest({
			host: "www.googleapis.com",
			path: `/drive/v2/files/${fileId}?alt=media`,
			headers: this._getHeaders()
		}).then(function(res) {
			if(res.statusCode !== 200) {
				return utils.streamToBuffer(res).then(function(body) {
					try {
						body = JSON.parse(body);
					} catch(e) {
						// Leave body as-is
					}
					throw new DownloadError(
						`Got response with status ${res.statusCode} and body ${inspect(body)}`);
				});
			} else {
				return res;
			}
		});
	}
}

GDriver.prototype.createFile = Promise.coroutine(GDriver.prototype.createFile);
GDriver.prototype.loadCredentials = Promise.coroutine(GDriver.prototype.loadCredentials);
GDriver.prototype.saveCredentials = Promise.coroutine(GDriver.prototype.saveCredentials);
GDriver.prototype.deleteFile = Promise.coroutine(GDriver.prototype.deleteFile);
GDriver.prototype.getMetadata = Promise.coroutine(GDriver.prototype.getMetadata);
GDriver.prototype.getData = Promise.coroutine(GDriver.prototype.getData);
GDriver.prototype._maybeRefreshAndSaveToken = Promise.coroutine(GDriver.prototype._maybeRefreshAndSaveToken);

module.exports = {GDriver};
