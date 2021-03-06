"use strict";

const fs        = require('fs');
const promisify = require('util').promisify;

module.exports = {
	chmodAsync:        promisify(fs.chmod),
	closeAsync:        promisify(fs.close),
	futimesAsync:      promisify(fs.futimes),
	openAsync:         promisify(fs.open),
	readFileAsync:     promisify(fs.readFile),
	renameAsync:       promisify(fs.rename),
	statAsync:         promisify(fs.stat),
	ftruncateAsync:    promisify(fs.ftruncate),
	unlinkAsync:       promisify(fs.unlink),
	writeFileAsync:    promisify(fs.writeFile),

	createReadStream:  fs.createReadStream,
	createWriteStream: fs.createWriteStream,

	readFileSync:      fs.readFileSync,
	writeFileSync:     fs.writeFileSync,
	appendFileSync:    fs.appendFileSync,
	existsSync:        fs.existsSync,
	readdirSync:       fs.readdirSync,
	renameSync:        fs.renameSync
};
