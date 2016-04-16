"use strict";

const fs = require('fs');
const Promise = require('bluebird');

// Promisify only the functions we need to avoid a 24+ ms require
// penalty with promisifyAll (tested Intel 4790K)
module.exports = {
	chmodAsync: Promise.promisify(fs.chmod),
	closeAsync: Promise.promisify(fs.close),
	futimesAsync: Promise.promisify(fs.futimes),
	openAsync: Promise.promisify(fs.open),
	readFileAsync: Promise.promisify(fs.readFile),
	renameAsync: Promise.promisify(fs.rename),
	statAsync: Promise.promisify(fs.stat),
	truncateAsync: Promise.promisify(fs.truncate),
	unlinkAsync: Promise.promisify(fs.unlink),
	writeFileAsync: Promise.promisify(fs.writeFile),

	createReadStream: fs.createReadStream,
	createWriteStream: fs.createWriteStream,

	readFileSync: fs.readFileSync,
	writeFileSync: fs.writeFileSync,
	existsSync: fs.existsSync,
	readdirSync: fs.readdirSync
};
