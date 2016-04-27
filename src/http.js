"use strict";

const A = require('ayy');
const T = require('notmytype');
const http = require('http');

function handleRequest(request, response) {
	response.end('Hello');
}

function listen(host, port, stashes) {
	T(host, T.string, port, T.number, stashes, T.list(T.string));
	var server = http.createServer(handleRequest);
	server.listen(port, host);
	console.log(`HTTP server listening on ${host}:${port}`);
}

module.exports = {listen};
