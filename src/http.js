"use strict";

const A         = require('ayy');
const T         = require('notmytype');
const http      = require('http');
const escape    = require('escape-html');
const terastash = require('.');
const utils     = require('./utils');
const mime      = require('mime-types');
const domain    = require('domain');

class StashServer {
	constructor(stashes) {
		T(stashes, T.list(T.string));
		this.stashes = new Set(stashes);
		this.client = terastash.getNewClient();
	}

	async _writeListing(res, stashInfo, parent) {
		const rows = await terastash.getChildrenForParent(
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
		for (const row of rows) {
			const d = row.type === "d" ? "/" : "";
			res.write(`
				<tr>
					<td><a href="${encodeURIComponent(row.basename) + d}">${escape(row.basename) + d}</a></td>
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

	async _handleRequest(req, res) {
		console.log(`OPENED: ${req.method} ${req.url} ${req.headers.range}`);
		res.setHeader("X-Frame-Options",        "DENY");
		res.setHeader("X-Content-Type-Options", "nosniff");
		res.setHeader("X-XSS-Protection",       "1; mode=block");
		res.setHeader("X-UA-Compatible",        "IE=edge");
		if (req.url === '/') {
			res.setHeader("Content-Type", "text/html; charset=utf-8");
			for (const stash of this.stashes) {
				res.write(`<li><a href="${encodeURIComponent(stash)}/">${escape(stash)}</a>\n`);
			}
			res.end();
		} else if (req.url === '/favicon.ico') {
			res.end();
		} else {
			let [_, stashName, dbPath] = utils.splitString(req.url, '/', 2);
			dbPath = decodeURIComponent(dbPath.replace(/\/+$/g, ""));
			A.eq(_, "");
			A(this.stashes.has(stashName), `Stash ${stashName} not in whitelist ${this.stashes}`);
			const stashInfo = await terastash.getStashInfoByName(stashName);
			let parent;
			// TODO: fix getRowByPath
			if (dbPath === "") {
				parent      = {};
				parent.uuid = Buffer.alloc(128/8);
				parent.type = "d";
			} else {
				parent = await terastash.getRowByPath(this.client, stashInfo.name, dbPath, ['type', 'uuid', 'size']);
			}
			if (parent.type === "d") {
				// If no trailing slash, redirect with trailing slash to avoid broken links
				if (!req.url.endsWith("/")) {
					res.statusCode = 302;
					res.setHeader("Location", req.url + "/");
					res.end();
				} else {
					this._writeListing(res, stashInfo, parent);
				}
			} else {
				// streamFile only supports 1 range anyway
				let firstRange = null;
				if (req.headers.range) {
					const matches = req.headers.range.match(/^bytes=(\d+)-(\d+)?/);
					const start   = matches[1];
					const end     = matches[2];
					if (start !== undefined) {
						firstRange = [
							parseInt(start, 10),
							end !== undefined ? parseInt(end, 10) : Number(parent.size)
						];
					}
				}
				let mimeType = mime.lookup(dbPath) || "application/octet-stream";
				// Don't let active content execute on this domain
				if (mimeType === "text/html") {
					mimeType = "text/plain";
				}
				res.setHeader("Content-Length", String(Number(parent.size)));
				res.setHeader("Accept-Ranges",  "bytes");
				res.setHeader("Content-Type",   mimeType);
				if (firstRange) {
					// Even if we're sending the whole file after a bytes=0-, the client should
					// get a 206 response so that they know they can do Range requests.
					// (e.g. mpv will refuse to seek unless it gets a 206?).
					res.statusCode = 206;
					res.setHeader("Content-Range", `bytes ${firstRange[0]}-${firstRange[1] - 1}/${Number(parent.size)}`);
				}
				// Too bad streamFile doesn't just take an uuid
				const [parentPath, basename] = utils.rsplitString(dbPath, '/', 1);
				const fileParent = await terastash.getUuidForPath(this.client, stashInfo.name, parentPath);
				const [_row, dataStream] = await terastash.streamFile(this.client, stashInfo, fileParent, basename, firstRange ? [firstRange] : undefined);
				// If the connection is closed by the client or the response just finishes, send an
				// .destroy() up the chain of streams, which will eventually abort the HTTPS request
				// made to Google.
				res.once('finish', function() {
					console.log(`CLOSED: ${req.method} ${req.url} ${req.headers.range}`);
					dataStream.destroy();
				});
				res.once('close', function() {
					console.log(`CLOSED: ${req.method} ${req.url} ${req.headers.range}`);
					dataStream.destroy();
				});
				utils.pipeWithErrors(dataStream, res);
			}
		}
	}

	async handleRequest(req, res) {
		try {
			return await this._handleRequest(req, res);
		} catch(err) {
			console.error(err.stack);
			res.statusCode = 500;
			res.end();
		}
	}
}

// We need a domain to avoid blowing up the whole process when something goes badly for one request
const d = domain.create();
d.on('error', function(err) {
	console.error(err.stack);
});

function listen(host, port, stashes) {
	T(host, T.string, port, T.number, stashes, T.list(T.string));
	d.run(function() {
		const stashServer = new StashServer(stashes);
		const httpServer = http.createServer(stashServer.handleRequest.bind(stashServer));
		httpServer.listen(port, host);
		console.log(`HTTP server listening on ${host}:${port}`);
	});
}

module.exports = {listen};
