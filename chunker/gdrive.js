"use strong";
"use strict";

const google = require('googleapis');
const Promise = require('bluebird');
const T = require('notmytype');
const OAuth2 = google.auth.OAuth2;
const basedir = require('xdg').basedir;

const REDIRECT_URL = 'urn:ietf:wg:oauth:2.0:oob';

function getOAuth2Client(clientId, clientSecret, credentials) {
	T(clientId, T.string, clientSecret, T.string, credentials, T.optional(T.object));
	const oauth2Client = new OAuth2(clientId, clientSecret, REDIRECT_URL);
	if(credentials) {
		oauth2Client.setCredentials(credentials);
	}
	return oauth2Client;
}

function getAuthUrl(oauth2Client) {
	T(oauth2Client, OAuth2);
	const scopes = ['https://www.googleapis.com/auth/drive'];
	const url = oauth2Client.generateAuthUrl({
		access_type: 'offline', // 'online' (default) or 'offline' (gets refresh_token)
		scope: scopes
	});
	return url;
}

function getCredentials(oauth2Client) {
	T(oauth2Client, OAuth2);
	return oauth2Client.credentials;
}

/**
 * Hit Google to get an access and refresh token based on `authCode`
 * and set the tokens on the `oauth2Client` object.
 *
 * Returns a Promise that resolves with the tokens
 */
function importAuthCode(oauth2Client, authCode) {
	T(oauth2Client, OAuth2, authCode, T.string);
	return new Promise(function(resolve, reject) {
		oauth2Client.getToken(authCode, function(err, tokens) {
			if(err) {
				reject(err);
			} else {
				oauth2Client.setCredentials(tokens);
				resolve(null);
			}
		});
	});
}

/**
 * Returns a Promise that resolves with the tokens
 */
function refreshTokens(oauth2Client) {
	T(oauth2Client, OAuth2);
	return new Promise(function(resolve, reject) {
		oauth2Client.refreshAccessToken(function(err, tokens) {
			if(err) {
				reject(err);
			} else {
				resolve(tokens);
			}
		});
	});
}

function areTokensExpired(oauth2Client) {
	T(oauth2Client, OAuth2);
	const expiryDate = oauth2Client.credentials.expiry_date;
	return expiryDate <= Date.now();
}

/*
function writeTokens(oauth2Client) {
	const configPath = basedir.configPath("terastash.json");
	mkdirp(path.dirname(configPath));
	fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
}*/

// TODO: allow specifying parent folder
function createFolder(oauth2Client, name) {
	T(oauth2Client, OAuth2, name, T.string);
	// We refresh the tokens ourselves so that we can capture them
	// and store them in a file.
	// XXX

	const drive = google.drive({version: 'v2', auth: oauth2Client});
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

module.exports = {getOAuth2Client, getAuthUrl, getCredentials, importAuthCode, createFolder};
