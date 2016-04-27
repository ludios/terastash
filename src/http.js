"use strict";

const A = require('ayy');
const T = require('notmytype');
const http = require('http');

class StashServer {
	constructor(stashes) {
		T(stashes, T.list(T.string));
		this.stashes = stashes;
	}

	_handleRequest(req, res) {
		res.setHeader("X-Frame-Options", "DENY");
		res.setHeader("X-Content-Type-Options", "nosniff");
		res.setHeader("X-XSS-Protection", "1; mode=block");
		res.setHeader("X-UA-Compatible", "IE=edge");
		if(req.url === '/') {
			res.setHeader("Content-Type", "text/html; charset=utf-8");
			for(const stash of this.stashes) {
				res.write(`<li><a href="${stash}/">${stash}</a>`);
			}
		} else {
			res.write('404');
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

function listen(host, port, stashes) {
	T(host, T.string, port, T.number, stashes, T.list(T.string));
	const stashServer = new StashServer(stashes);
	const httpServer = http.createServer(stashServer.handleRequest.bind(stashServer));
	httpServer.listen(port, host);
	console.log(`HTTP server listening on ${host}:${port}`);
}

module.exports = {listen};
