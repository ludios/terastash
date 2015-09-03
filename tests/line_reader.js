"use strong";
"use strict";

require('better-buffer-inspect');

const A = require('ayy');
const T = require('notmytype');
const utils = require('../utils');
const line_reader = require('../line_reader');
const streamifier = require('streamifier');
const Promise = require('bluebird');

const pow = Math.pow;

function makeLines(lineLength) {
	let n = 1000;
	const bufs = [];
	const buf = new Buffer("X".repeat(lineLength));
	const LF = new Buffer("\n");
	while(n--) {
		bufs.push(buf);
		bufs.push(LF);
	}
	if(Math.random() < 0.5) {
		// Results should be the same whether or not the last LF is present
		bufs.pop();
	}
	return Buffer.concat(bufs);
}

describe('DelimitedBufferDecoder', function() {
	it("can split lines", Promise.coroutine(function*() {
		for(const lineLength of [1, 2, 4, 10, 32, 63, 64, 65, 255, 256, 8 * 1024, pow(2, 16 - 1), pow(2, 16), pow(2, 16) + 1]) {
			const inputBuf = makeLines(lineLength);
			const inputStream = streamifier.createReadStream(inputBuf);
			const lineStream = new line_reader.DelimitedBufferDecoder(new Buffer("\n"));
			utils.pipeWithErrors(inputStream, lineStream);
			const lines = [];
			lineStream.on('data', function(line) {
				T(line, Buffer);
				A.eq(line.length, lineLength);
				lines.push(line);
			});
			yield new Promise(function(resolve) {
				lineStream.on('end', function() {
					resolve();
				});
			});
			A.eq(lines.length, 1000);
		}
	}));
});
