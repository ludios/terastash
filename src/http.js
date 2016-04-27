"use strict";

const A = require('ayy');
const T = require('notmytype');
const http = require('http');
const escape = require('escape-html');
const Promise = require('bluebird');
const terastash = require('.');
const utils = require('./utils');

class StashServer {
	constructor(stashes) {
		T(stashes, T.list(T.string));
		this.stashes = new Set(stashes);
		this.client = terastash.getNewClient();
	}

	*_handleRequest(req, res) {
		res.setHeader("X-Frame-Options", "DENY");
		res.setHeader("X-Content-Type-Options", "nosniff");
		res.setHeader("X-XSS-Protection", "1; mode=block");
		res.setHeader("X-UA-Compatible", "IE=edge");
		if(req.url === '/') {
			res.setHeader("Content-Type", "text/html; charset=utf-8");
			for(const stash of this.stashes) {
				res.write(`<li><a href="${escape(stash)}/">${escape(stash)}</a>\n`);
			}
		} else if(req.url === '/favicon.ico') {
			res.end();
		} else {
			let [_, stashName, dbPath] = utils.splitString(req.url, '/', 2);
			dbPath = decodeURIComponent(dbPath.replace(/\/+$/g, ""));
			A.eq(_, "");
			A(this.stashes.has(stashName), `Stash ${stashName} not in whitelist ${this.stashes}`);
			const stashInfo = yield terastash.getStashInfoByName(stashName);
			const parent = yield terastash.getUuidForPath(this.client, stashInfo.name, dbPath);
			const rows = yield terastash.getChildrenForParent(
				this.client, stashInfo.name, parent,
				["basename", "type", "size", "mtime", "executable"]
			);
			res.setHeader("Content-Type", "text/html; charset=utf-8");
			res.write(`
				<!doctype html>
				<html>
				<body>
				<style>
					a {
						text-decoration: none;
					}
				</style>
				<li><a href="../">../</a>
			`);
			for(const row of rows) {
				const d = row.type === "d" ? "/" : "";
				res.write(`<li><a href="${escape(row.basename) + d}">${escape(row.basename) + d}</a>\n`);
			}
			res.write(`
				</body>
				</html>
			`);
		}
		res.end();
	}

	handleRequest(req, res) {
		try {
			return this._handleRequest(req, res);
		} catch(err) {
			console.error(err.stack);
			res.end();
		}
	}
}

StashServer.prototype._handleRequest = Promise.coroutine(StashServer.prototype._handleRequest);

function listen(host, port, stashes) {
	T(host, T.string, port, T.number, stashes, T.list(T.string));
	const stashServer = new StashServer(stashes);
	const httpServer = http.createServer(stashServer.handleRequest.bind(stashServer));
	httpServer.listen(port, host);
	console.log(`HTTP server listening on ${host}:${port}`);
}

module.exports = {listen};
