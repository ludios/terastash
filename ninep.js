"use strong";
"use strict";

const A = require('ayy');
const T = require('notmytype');
const Promise = require('bluebird');
const net = require('net');
const inspect = require('util').inspect;
const utils = require('./utils');
const frame_reader = require('./frame_reader');

const packets = {
	100: {name: "Tversion", fmt: ["i4:msize", "S2:version"]},
	101: {name: "Rversion", fmt: ["i4:msize", "S2:version"]},
	102: {name: "Tauth", fmt: ["S2:uname", "S2:aname"]},
	103: {name: "Rauth", fmt: ["b13:aqid"]},
	104: {name: "Tattach", fmt: ["i4:fid", "i4:afid", "S2:uname", "S2:aname"]},
	105: {name: "Rattach", fmt: ["b13:qid"]},
	107: {name: "Rerror", fmt: ["S2:ename"]},
	108: {name: "Tflush", fmt: ["i2:oldtag"]},
	109: {name: "Rflush", fmt: []},
	110: {name: "Twalk", fmt: ["i4:fid", "i4:newfid", "i2:nwname", "R:wname"]},
	111: {name: "Rwalk", fmt: ["i2:nqid", "R:qids"]},
	112: {name: "Topen", fmt: ["i4:fid", "i1:mode"]},
	113: {name: "Ropen", fmt: ["b13:qid", "i4:iounit"]},
	114: {name: "Tcreate", fmt: ["i4:fid", "S2:name", "i4:perm", "i1:mode"]},
	115: {name: "Rcreate", fmt:["i13:qid", "i4:iounit"]},
	116: {name: "Tread", fmt: ["i4:fid", "i8:offset", "i4:count"]},
	117: {name: "Rread", fmt: ["S4:data"]},
	118: {name: "Twrite", fmt: ["i4:fid", "i8:offset", "S4:data"]},
	119: {name: "Rwrite", fmt: ["i4:count"]},
	120: {name: "Tclunk", fmt: ["i4:fid"]},
	121: {name: "Rclunk", fmt: []},
	122: {name: "Tremove", fmt: ["i4:fid"]},
	124: {name: "Tstat", fmt: ["i4:fid"]},
	125: {name: "Rstat", fmt: ["S2:stat"]},
	126: {name: "Twstat", fmt: ["i4:fid", "S2:stat"]}
};

const Type = {};
for(const p of Object.keys(packets)) {
	Type[packets[p].name] = Number(p);
}

function readString(frame, offset) {
	const size = frame.readUInt16LE(frame, offset);
	return frame.slice(offset + 2, offset + 2 + size);
}

const BuffersType = T.list(Buffer);

function reply(client, type, tag, bufs) {
	T(client, T.object, type, T.number, tag, T.number, bufs, BuffersType);
	const preBuf = new Buffer(7);
	let length = 0;
	for(const buf of bufs) {
		length += buf.length;
	}
	preBuf.writeUInt32LE(7 + length, 0);
	preBuf.writeUInt8(type, 4);
	preBuf.writeUInt16LE(tag, 5);
	client.cork();
	client.write(preBuf);
	for(const buf of bufs) {
		client.write(buf);
	}
	client.uncork();
	console.error("<-", packets[type].name, {tag, bufs});
}

function uint32(n) {
	T(n, T.number);
	const buf = new Buffer(4);
	buf.writeUInt32LE(n, 0);
	return buf;
}

function uint16(n) {
	T(n, T.number);
	const buf = new Buffer(2);
	buf.writeUInt16LE(n, 0);
	return buf;
}

function string(b) {
	T(b, Buffer);
	A.lte(b.length, 64 * 1024);
	return [uint16(b.length), b];
}

function listen(socketPath) {
	T(socketPath, T.string);
	const ourMax = (64 * 1024 * 1024) - 4;
	const server = net.createServer(function(client) {
		const decoder = new frame_reader.Int32BufferDecoder("LE", ourMax, true);
		utils.pipeWithErrors(client, decoder);
		decoder.on('data', function(frame) {
			const type = frame.readUInt8(frame, 0);
			const tag = frame.readUInt16LE(frame, 1);
			if(type === Type.Tversion) {
				const msize = frame.readUInt32LE(frame, 3);
				const version = readString(frame, 7);
				console.error("->", packets[type].name, {tag, msize, version});
				// http://man.cat-v.org/plan_9/5/version - we must respond
				// with an equal or smaller msize.  Note that msize includes
				// the size int itself.
				const replyMsize = Math.min(msize, ourMax + 4);
				console.error("<- Rversion", {tag, replyMsize, version});
				reply(client, Type.Rversion, tag, [uint32(replyMsize)].concat(string(version)));
			} else {
				console.error("-> Unknown message", {frame, type, tag});
			}
		});
	});
	server.listen(socketPath);
	console.log(`9P server started, listening on UNIX domain socket at ${inspect(socketPath)}`);
}

module.exports = {listen};
