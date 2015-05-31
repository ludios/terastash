"use strong";
"use strict";

const fs = require('fs');
const google = require('googleapis');
const path = require('path');
const Promise = require('bluebird');
const T = require('notmytype');
const mkdirp = require('mkdirp');
const OAuth2 = google.auth.OAuth2;
const basedir = require('xdg').basedir;

const REDIRECT_URL = 'urn:ietf:wg:oauth:2.0:oob';

class GDriver {
	/* previously getOAuth2Client*/
	constructor(clientId, clientSecret, credentials) {
		T(clientId, T.string, clientSecret, T.string, credentials, T.optional(T.object));
		this.clientId = clientId;
		this.clientSecret = clientSecret;
		this._oauth2Client = new OAuth2(clientId, clientSecret, REDIRECT_URL);
		if(credentials) {
			this._oauth2Client.setCredentials(credentials);
		}
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
	 * Returns a Promise that resolves with the tokens
	 */
	importAuthCode(authCode) {
		T(authCode, T.string);
		return new Promise(function(resolve, reject) {
			this._oauth2Client.getToken(authCode, function(err, tokens) {
				if(err) {
					reject(err);
				} else {
					this._oauth2Client.setCredentials(tokens);
					resolve(null);
				}
			});
		});
	}

	writeCredentials(allCredentials) {
		T(allCredentials, T.object);
		const tokensPath = basedir.configPath(path.join("terastash", "google-tokens.json"));
		mkdirp(path.dirname(tokensPath));
		fs.writeFileSync(tokensPath, JSON.stringify(allCredentials, null, 2));
	}

	readCredentials() {
		const tokensPath = basedir.configPath(path.join("terastash", "google-tokens.json"));
		try {
			return JSON.parse(fs.readFileSync(tokensPath));
		} catch(e) {
			if(e.code !== 'ENOENT') {
				throw e;
			}
			// If there is no config file, write one.
			const allCredentials = {
				credentials: {},
				_comment: "Access tokens expire quickly; refresh tokens never expire unless revoked."
			};
			this.writeCredentials(allCredentials);
			return allCredentials;
		}
	}

	updateCredential(clientId, credentials) {
		T(credentials, T.object);
		const allCredentials = this.readCredentials();
		allCredentials.credentials[clientId] = credentials;
		this.writeCredentials(allCredentials);
	}

	getCredential(clientId) {
		return this.readCredentials().credentials[clientId] || null;
	}

	// TODO: allow specifying parent folder
	createFolder(name) {
		T(name, T.string);
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
		});
	}
}

module.exports = {GDriver};
