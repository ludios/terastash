"use strict";

require('better-buffer-inspect');

const A                     = require('ayy');
const T                     = require('notmytype');
const utils                 = require('../utils');
const line_reader           = require('../line_reader');
const realistic_streamifier = require('../realistic_streamifier');
const PassThrough           = require('stream').PassThrough;

const pow = Math.pow;

function makeLines(lineLength, numLines) {
	T(lineLength, T.number, numLines, T.number);
	const bufs = [];
	const buf = Buffer.from("X".repeat(lineLength));
	const LF = Buffer.from("\n");
	while(numLines--) {
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
	it("can split lines", async function() {
		for(const lineLength of [1, 4, 32, 63, 65, 255, 256, 8 * 1024, pow(2, 16 - 1), pow(2, 16), pow(2, 16) + 1, 15*1000*1000]) {
			//console.log({lineLength});
			const numLines =
				lineLength < 100000 ?
					1000 :
					3;
			const inputBuf          = makeLines(lineLength, numLines);
			const inputStream       = realistic_streamifier.createReadStream(inputBuf);
			const passThroughStream = new PassThrough();
			utils.pipeWithErrors(inputStream, passThroughStream);
			const lineStream        = new line_reader.DelimitedBufferDecoder(Buffer.from("\n"));
			utils.pipeWithErrors(passThroughStream, lineStream);
			const lines = [];
			lineStream.on('data', function(line) {
				T(line, Buffer);
				A.eq(line.length, lineLength);
				lines.push(line);
			});
			await new Promise(function(resolve) {
				lineStream.on('end', function() {
					resolve();
				});
			});
			A.eq(lines.length, numLines);
		}
	});
});
