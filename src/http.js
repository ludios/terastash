"use strict";

const A = require('ayy');
const T = require('notmytype');
const http = require('http');
const escape = require('escape-html');
const Promise = require('bluebird');
const terastash = require('.');
const utils = require('./utils');
const mime = require('mime-types');

class StashServer {
	constructor(stashes) {
		T(stashes, T.list(T.string));
		this.stashes = new Set(stashes);
		this.client = terastash.getNewClient();
	}

	*_writeListing(res, stashInfo, parent) {
		const rows = yield terastash.getChildrenForParent(
			this.client, stashInfo.name, parent.uuid,
			["basename", "type", "size", "mtime", "executable"]
		);
		res.setHeader("Content-Type", "text/html; charset=utf-8");
		res.write(`
			<!doctype html>
			<html>
			<body>
			<style>
				body, td {
					font-family: sans-serif;
				}
				a {
					text-decoration: none;
				}
				table, td {
					border: 0;
				}
				td.size {
					text-align: right;
				}
			</style>
			<table>
			<tr>
				<td>Name</td>
				<td>Last modified</td>
				<td class="size">Size</td>
			</tr>
			<tr>
				<td><a href="../">../</a></td>
				<td>-</td>
				<td class="size">-</td>
			</tr>
		`);
		for(const row of rows) {
			const d = row.type === "d" ? "/" : "";
			res.write(`
				<tr>
					<td><a href="${escape(row.basename) + d}">${escape(row.basename) + d}</a></td>
					<td>${utils.shortISO(row.mtime)}</td>
					<td class="size">${row.size != null ? utils.commaify(Number(row.size)) : "-"}</td>
				</tr>
			`);
		}
		res.write(`
			</table>
			</body>
			</html>
		`);
		res.end();
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
			res.end();
		} else if(req.url === '/favicon.ico') {
			res.end();
		} else {
			let [_, stashName, dbPath] = utils.splitString(req.url, '/', 2);
			dbPath = decodeURIComponent(dbPath.replace(/\/+$/g, ""));
			A.eq(_, "");
			A(this.stashes.has(stashName), `Stash ${stashName} not in whitelist ${this.stashes}`);
			const stashInfo = yield terastash.getStashInfoByName(stashName);
			let parent;
			// TODO: fix getRowByPath
			if(dbPath === "") {
				parent = {};
				parent.uuid = new Buffer(128/8).fill(0);
				parent.type = "d";
			} else {
				parent = yield terastash.getRowByPath(this.client, stashInfo.name, dbPath, ['type', 'uuid']);
			}
			if(parent.type === "d") {
				this._writeListing(res, stashInfo, parent);
			} else {
				const mimeType = mime.lookup(dbPath) || "application/octet-stream";
				// Don't let active content execute on this domain
				if(mimeType === "text/html") {
					mimeType = "text/plain";
				}
				res.setHeader("Content-Type", mimeType);
				// Too bad streamFile doesn't just take an uuid
				const [parentPath, basename] = utils.rsplitString(dbPath, '/', 1);
				const fileParent = yield terastash.getUuidForPath(this.client, stashInfo.name, parentPath);
				const [row, dataStream] = yield terastash.streamFile(this.client, stashInfo, fileParent, basename);
				utils.pipeWithErrors(dataStream, res);
				//res.end();
			}
		}
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

StashServer.prototype._writeListing = Promise.coroutine(StashServer.prototype._writeListing);
StashServer.prototype._handleRequest = Promise.coroutine(StashServer.prototype._handleRequest);

function listen(host, port, stashes) {
	T(host, T.string, port, T.number, stashes, T.list(T.string));
	const stashServer = new StashServer(stashes);
	const httpServer = http.createServer(stashServer.handleRequest.bind(stashServer));
	httpServer.listen(port, host);
	console.log(`HTTP server listening on ${host}:${port}`);
}

module.exports = {listen};
