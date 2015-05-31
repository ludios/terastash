"use strong";
"use strict";

const google = require('googleapis');
const Promise = require('bluebird');
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

class UploadError extends Error {
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
		this._oauth2Client._realCredentials = this._oauth2Client.credentials;
		const that = this;
		// Replace OAuth2Client.credentials with a setter that automatically
		// saves the credentials.  We do this instead of saving the credentials
		// after a request, because a request may be retried by googleapis internally
		// if the credentials are expired, and we want to capture the updated
		// credentials as soon as possible so that other processes can use them.
		Object.defineProperty(this._oauth2Client, 'credentials', {
			get: function() {
				return this._realCredentials;
			},
			set: function(credentials) {
				this._realCredentials = credentials;
				that.saveCredentials().catch(function(err) {
					console.log("Error in GDriver.saveCredentials():");
					console.error(err.stack);
				});
			},
			enumerable: true,
			configurable: false
		});
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
					this._oauth2Client._realCredentials = tokens;
					resolve(this.saveCredentials());
				}
			}.bind(this));
		}.bind(this));
	}

	*loadCredentials() {
		const config = yield getAllCredentials();
		const credentials = config.credentials[this.clientId];
		if(credentials) {
			this._oauth2Client._realCredentials = credentials;
		}
	}

	*saveCredentials() {
		const config = yield getAllCredentials();
		config.credentials[this.clientId] = this._oauth2Client.credentials;
		//console.log("Saving credentials", this._oauth2Client.credentials);
		return utils.writeObjectToConfigFile("google-tokens.json", config);
	}

	// TODO: allow specifying parent folder
	createFolder(name, requestCb) {
		T(name, T.string, requestCb, T.optional(T.object));
		const drive = google.drive({version: 'v2', auth: this._oauth2Client});
		return new Promise(function(resolve, reject) {
			drive.files.insert({
				resource: {
					title: name,
					mimeType: 'application/vnd.google-apps.folder'
				}
			}, function(err, obj) {
				if(err) {
					reject(err);
				} else {
					resolve(obj);
				}
			});
			if(requestCb) {
				requestCb(requestObj);
			}
		});
	}

	// TODO: allow specifying parent folder
	/**
	 * Returns a Promise that is resolved with the response from Google,
	 * mostly importantly containing an "id" property with the file ID that
	 * Google has assigned to it.
	 */
	createFile(name, opts, stream, requestCb) {
		T(
			name, T.string,
			opts, T.shape({parents: T.optional(T.list(T.string))}),
			stream, T.object,
			requestCb, T.optional(T.object)
		);

		const md5 = crypto.createHash('md5');
		let length = 0;
		const passthrough = new PassThrough();
		stream.pipe(passthrough);
		passthrough.on('data', function(data) {
			length += data.length;
			md5.update(data);
		});

		const drive = google.drive({version: 'v2', auth: this._oauth2Client});
		return new Promise(function(resolve, reject) {
			const requestObj = drive.files.insert({
				resource: {
					title: name,
					parents: opts.parents && opts.parents.map(function(parentId) {
						return {
							"kind": "drive#fileLink",
							"id": parentId
						};
					}),
					mimeType: 'application/octet-stream'
				},
				media: {
					mimeType: 'application/octet-stream',
					body: passthrough
				}
			}, function(err, obj) {
				if(err) {
					reject(err);
				} else {
					resolve(obj);
				}
			});
			if(requestCb) {
				requestCb(requestObj);
			}
		}).then(function(obj) {
			T(obj, T.object);
			if(obj.kind !== "drive#file") {
				throw new UploadError(`Expected Google Drive to create an` +
					` object with kind='drive#file' but was ${inspect(obj.kind)}`
				);
			}
			if(obj.fileSize !== String(length)) {
				throw new UploadError(`Expected Google Drive to create a` +
					` file with fileSize=${inspect(String(length))} but was ${inspect(obj.fileSize)}`
				);
			}
			const expectedHexDigest = md5.digest('hex');
			if(obj.md5Checksum !== expectedHexDigest) {
				throw new UploadError(`Expected Google Drive to create a` +
					` file with md5Checksum=${inspect(expectedHexDigest)}` +
					` but was ${inspect(obj.md5Checksum)}`
				);
			}
			return obj;
		});
	}
}

GDriver.prototype.loadCredentials = Promise.coroutine(GDriver.prototype.loadCredentials);
GDriver.prototype.saveCredentials = Promise.coroutine(GDriver.prototype.saveCredentials);

module.exports = {GDriver};
