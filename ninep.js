"use strong";
"use strict";

const A = require('ayy');
const T = require('notmytype');
const Promise = require('bluebird');
const net = require('net');
const inspect = require('util').inspect;
const utils = require('./utils');
const crypto = require('crypto');
const getProp = utils.getProp;
const terastash = require('./');
const frame_reader = require('./frame_reader');
const chalk = require('chalk');

function _buf(size) {
	return function(buf) {
		A.eq(buf.length, size);
		return buf;
	};
}

/* 9P2000.L types */
const QT = {
	DIR: 0x80, // directory
	APPEND: 0x40, // append-only file
	EXCL: 0x20, // exclusive use file
	MOUNT: 0x10, // mounted channel
	AUTH: 0x08, // authentication file
	TMP: 0x04, // non-backed-up file
	LINK: 0x02, // symbolic link
	FILE: 0x00 // regular file
};

/* VFS types from dirent.h */
const DT = {
	 UNKNOWN: 0
	,FIFO: 1
	,CHR: 2
	,DIR: 4
	,BLK: 6
	,REG: 8
	,LNK: 10
	,SOCK: 12
	,WHT: 14
}

/* stat mode bits from http://osxr.org/glibc/source/sysdeps/unix/sysv/linux/x86/bits/stat.h#0182 */
const STAT = {
	 IFDIR: 0o0040000 /* Directory */
	,IFCHR: 0o0020000 /* Character device */
	,IFBLK: 0o0060000 /* Block device */
	,IFREG: 0o0100000 /* Regular file */
	,IFIFO: 0o0010000 /* FIFO */
	,IFLNK: 0o0120000 /* Symbolic link */
	,IFSOCK: 0o0140000 /* Socket */
}

//console.error({fidMap: this._fidMap, qidMap: this._qidMap});

function _enc_Rwalk(obj) {
	const wqids = obj.wqids;
	return [uint16(wqids.length)].concat(wqids.map(_qid));
}

function _enc_Rreaddir(obj) {
	const bufs = [null];
	let count = 0;
	for(const entry of obj.entries) {
		const buf = Buffer.concat([
			_qid(entry.qid),
			uint64(entry.offset),
			uint8(entry.type === "FILE" ? DT.REG : DT.DIR),
			string(entry.name)
		]);
		bufs.push(buf);
		count += buf.length;
	}
	bufs[0] = uint32(count);
	return bufs;
}

// Note: fmt: is for documentation only
const packets = {
	// https://github.com/chaos/diod/blob/master/protocol.md
	// http://lxr.free-electrons.com/source/include/net/9p/9p.h
	8: {name: "Tstatfs"},
	9: {name: "Rstatfs"},
	12: {name: "Tlopen"}, // fid[4] flags[4]
	13: {name: "Rlopen", enc: ["qid", _qid, "iounit", uint32]},
	14: {name: "Tlcreate"},
	15: {name: "Rlcreate"},
	24: {name: "Tgetattr"}, // tag[2] fid[4] request_mask[8]
	25: {name: "Rgetattr", enc: [
		"valid", _buf(8),
		"qid", _qid,
		"mode", uint32,
		"uid", uint32,
		"gid", uint32,
		"nlink", uint64,
		"rdev", _buf(8),
		"size", _buf(8),
		"blksize", _buf(8),
		"blocks", _buf(8),
		"atime_sec", _buf(8),
		"atime_nsec", _buf(8),
		"mtime_sec", _buf(8),
		"mtime_nsec", _buf(8),
		"ctime_sec", _buf(8),
		"ctime_nsec", _buf(8),
		"btime_sec", _buf(8),
		"btime_nsec", _buf(8),
		"gen", _buf(8),
		"data_version", _buf(8)
	]},
	26: {name: "Tsetattr"},
	27: {name: "Rsetattr"},
	30: {name: "Txattrwalk"}, // fid[4] newfid[4] name[s]
	31: {name: "Rxattrwalk", enc: ["size", _buf(8)]},
	40: {name: "Treaddir"}, // fid[4] offset[8] count[4]
	41: {name: "Rreaddir", enc: _enc_Rreaddir}, // count[4] data[count]
	50: {name: "Tfsync"},
	51: {name: "Rfsync"},
	72: {name: "Tmkdir"},
	73: {name: "Rmkdir"},
	100: {name: "Tversion", fmt: ["i4:msize", "S2:version"]},
	101: {name: "Rversion", enc: ["msize", uint32, "version", string]},
	102: {name: "Tauth", fmt: ["S2:uname", "S2:aname"]},
	103: {name: "Rauth", fmt: ["b13:aqid"]},
	104: {name: "Tattach", fmt: ["i4:fid", "i4:afid", "S2:uname", "S2:aname"]},
	105: {name: "Rattach", enc: ["qid", _qid]},
	107: {name: "Rerror", fmt: ["S2:ename"]},
	108: {name: "Tflush", fmt: ["i2:oldtag"]},
	109: {name: "Rflush", fmt: []},
	110: {name: "Twalk", fmt: ["i4:fid", "i4:newfid", "i2:nwname", "R:wname"]},
	111: {name: "Rwalk", enc: _enc_Rwalk},
	112: {name: "Topen", fmt: ["i4:fid", "i1:mode"]},
	113: {name: "Ropen", fmt: ["b13:qid", "i4:iounit"]},
	114: {name: "Tcreate", fmt: ["i4:fid", "S2:name", "i4:perm", "i1:mode"]},
	115: {name: "Rcreate", fmt:["i13:qid", "i4:iounit"]},
	116: {name: "Tread", fmt: ["i4:fid", "i8:offset", "i4:count"]},
	117: {name: "Rread", fmt: ["S4:data"]},
	118: {name: "Twrite", fmt: ["i4:fid", "i8:offset", "S4:data"]},
	119: {name: "Rwrite", fmt: ["i4:count"]},
	120: {name: "Tclunk", fmt: ["i4:fid"]},
	121: {name: "Rclunk", enc: []},
	122: {name: "Tremove", fmt: ["i4:fid"]},
	124: {name: "Tstat", fmt: ["i4:fid"]},
	125: {name: "Rstat", fmt: ["S2:stat"]},
	126: {name: "Twstat", fmt: ["i4:fid", "S2:stat"]}
};

const Type = {};
for(const p of Object.keys(packets)) {
	Type[packets[p].name] = Number(p);
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

function uint64(n) {
	T(n, T.number);
	A.gte(n, 0);
	// TODO: support for 64-bit-wide
	A.lte(n, Math.pow(2, 32));
	const buf = new Buffer(8).fill(0);
	buf.writeUInt32LE(n);
	return buf;
}

function uint32(n) {
	T(n, T.number);
	A.gte(n, 0);
	A.lte(n, Math.pow(2, 32));
	const buf = new Buffer(4);
	buf.writeUInt32LE(n, 0);
	return buf;
}

function uint16(n) {
	T(n, T.number);
	A.gte(n, 0);
	A.lte(n, Math.pow(2, 16));
	const buf = new Buffer(2);
	buf.writeUInt16LE(n, 0);
	return buf;
}

function uint8(n) {
	T(n, T.number);
	A.gte(n, 0);
	A.lte(n, Math.pow(2, 8));
	const buf = new Buffer(1);
	buf.writeUInt8(n, 0);
	return buf;
}

function string(b) {
	T(b, Buffer);
	A.lte(b.length, 64 * 1024);
	return Buffer.concat([uint16(b.length), b]);
}

const QIDType = T.shape({type: T.string, version: T.number, path: Buffer});

function _qid(obj) {
	T(obj, QIDType);
	return Buffer.concat([uint8(QT[obj.type]), uint32(obj.version), obj.path]);
}

function makeQID(type, version, path) {
	T(type, T.number, version, T.number, path, Buffer);
	A.eq(path.length, 8);
	return Buffer.concat([uint8(type), uint32(version), path]);
}

class FrameReader {
	constructor(frame) {
		this._frame = frame;
		this._offset = 0;
	}

	string() {
		const size = this._frame.readUInt16LE(this._offset);
		this._offset += 2;
		const s = this._frame.slice(this._offset, this._offset + size);
		this._offset += size;
		return s;
	}

	uint32() {
		const int = this._frame.readUInt32LE(this._offset);
		this._offset += 4;
		return int;
	}

	uint16() {
		const int = this._frame.readUInt16LE(this._offset);
		this._offset += 2;
		return int;
	}

	uint8() {
		const int = this._frame.readUInt8(this._offset);
		this._offset += 1;
		return int;
	}

	buffer(length) {
		const buf = this._frame.slice(this._offset, this._offset + length);
		A.eq(buf.length, length);
		this._offset += length;
		return buf;
	}
}

function decodeMessage(frameBuf) {
	T(frameBuf, Buffer);
	const frame = new FrameReader(frameBuf);
	const type = frame.uint8();
	const tag = frame.uint16();
	if(type === Type.Tread) {
		const fid = frame.uint32();
		const offset = frame.buffer(8);
		const count = frame.uint32();
		return {type, tag, fid, offset, count};
	} else if(type === Type.Twrite) {
		const fid = frame.uint32();
		const offset = frame.buffer(8);
		const count = frame.uint32();
		const data = frame.buffer(count);
		return {type, tag, fid, offset, data};
	} else if(type === Type.Treaddir) {
		const fid = frame.uint32();
		const offset = frame.buffer(8);
		const count = frame.uint32();
		return {type, tag, fid, offset, count};
	} else if(type === Type.Tversion) {
		const msize = frame.uint32();
		const version = frame.string();
		return {type, tag, msize, version};
	} else if(type === Type.Tattach) {
		const fid = frame.uint32();
		const afid = frame.uint32();
		const uname = frame.string();
		const aname = frame.string();
		return {type, tag, fid, afid, uname, aname};
	} else if(type === Type.Tgetattr) {
		const fid = frame.uint32();
		const request_mask = frame.buffer(8);
		return {type, tag, fid, request_mask};
	} else if(type === Type.Tclunk) {
		const fid = frame.uint32();
		return {type, tag, fid};
	} else if(type === Type.Txattrwalk) {
		const fid = frame.uint32();
		const newfid = frame.uint32();
		const name = frame.string();
		return {type, tag, fid, newfid, name};
	} else if(type === Type.Twalk) {
		const fid = frame.uint32();
		const newfid = frame.uint32();
		const nwname = frame.uint16();
		const wnames = [];
		let n = nwname;
		while(n--) {
			wnames.push(frame.string());
		}
		return {type, tag, fid, newfid, wnames};
	} else if(type === Type.Tlopen) {
		const fid = frame.uint32();
		const flags = frame.uint32();
		return {type, tag, fid, flags};
	} else if(type === Type.Tlcreate) {
		const fid = frame.uint32();
		const name = frame.string();
		const flags = frame.uint32();
		const mode = frame.uint32();
		const gid = frame.uint32();
		return {type, tag, fid, name, flags, mode, gid};
	} else if(type === Type.Tmkdir) {
		const dfid = frame.uint32();
		const name = frame.string();
		const mode = frame.uint32();
		const gid = frame.uint32();
		return {type, tag, dfid, name, mode, gid};
	} else if(type === Type.Tsetattr) {
		const fid = frame.uint32();
		const valid = frame.uint32();
		const mode = frame.uint32();
		const uid = frame.uint32();
		const gid = frame.uint32();
		const size = frame.buffer(8);
		const atime_sec = frame.buffer(8);
		const atime_nsec = frame.buffer(8);
		const mtime_sec = frame.buffer(8);
		const mtime_nsec = frame.buffer(8);
		return {type, tag, fid, valid, mode, uid, gid, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec};
	} else if(type === Type.Tfsync) {
		const fid = frame.uint32();
		return {type, tag, fid};
	} else if(type === Type.Tstatfs) {
		const fid = frame.uint32();
		return {type, tag, fid};
	} else if(type === Type.Tflush) {
		const oldtag = frame.uint32();
		return {type, tag, oldtag};
	} else {
		return {type, tag, decode_error: "Unsupported message"};
	}
}


class Terastash9P {
	constructor(peer) {
		this._peer = peer;
		this._stashName = null;
		this._qidMap = new Map();
		this._fidMap = new Map();
		this._ourMax = (64 * 1024 * 1024) - 4;
		this._msize = null;
		this._client = terastash.getNewClient();
	}

	init() {
		const decoder = new frame_reader.Int32BufferDecoder("LE", this._ourMax, true);
		utils.pipeWithErrors(this._peer, decoder);
		decoder.on('data', this.handleFrame.bind(this));
		decoder.on('error', function(err) {
			console.error(err);
		});
		decoder.on('end', function() {
			console.log('Disconnected');
		});
	}

	replyAny(tag, type, bufs) {
		T(tag, T.number, type, T.number, bufs, T.list(Buffer));
		let length = 0;
		for(const buf of bufs) {
			length += buf.length;
		}
		const preBuf = new Buffer(4 + 1 + 2);
		const totalLength = 4 + 1 + 2 + length;
		A.lte(totalLength, this._msize);
		preBuf.writeUInt32LE(totalLength, 0);
		preBuf.writeUInt8(type, 4);
		preBuf.writeUInt16LE(tag, 5);
		this._peer.cork();
		this._peer.write(preBuf);
		for(const buf of bufs) {
			this._peer.write(buf);
		}
		this._peer.uncork();
	}

	replyOK(msg, obj) {
		T(msg, T.object, obj, T.object);
		const tag = msg.tag;
		const type = msg.type + 1;
		console.error(chalk.red(`<- ${packets[type].name}\n${inspect(Object.assign(obj, {tag}))}`));

		let bufs = [];
		if(packets[type].enc instanceof Array) {
			for(let i=0; i < packets[type].enc.length; i+=2) {
				const field = packets[type].enc[i];
				const encFn = packets[type].enc[i + 1];
				const buf = encFn(obj[field]);
				bufs.push(buf);
			}
		} else {
			bufs = packets[type].enc(obj);
		}
		this.replyAny(tag, type, bufs);
	}

	replyError(msg, reason) {
		const type = Type.Rerror;
		console.error(chalk.bold(chalk.red(`<- ${packets[type].name} ${inspect(reason)}`)));
		this.replyAny(msg.tag, type, [string(new Buffer(reason))]);
	}

	*handleMessage(msg) {
		console.error(chalk.cyan(`-> ${getProp(packets, String(msg.type), {name: "?"}).name}\n${inspect(msg)}`));
		if(msg.type === Type.Tversion) {
			// TODO: ensure version is 9P2000.L
			// http://man.cat-v.org/plan_9/5/version - we must respond
			// with an equal or smaller msize.  Note that msize includes
			// the size int itself.
			this._msize = Math.min(msg.msize, this._ourMax + 4);
			this.replyOK(msg, {msize: this._msize, version: msg.version});
		} else if(msg.type === Type.Tattach) {
			this._stashName = msg.aname.toString('utf-8');
			const qid = {type: "DIR", version: 0, path: new Buffer(8).fill(0)};
			// UUID 0000... is the root of the stash
			this._qidMap.set(_qid(qid).toString('hex'), new Buffer(128/8).fill(0));
			this._fidMap.set(msg.fid, qid);
			this.replyOK(msg, {qid});
		} else if(msg.type === Type.Tgetattr) {
			const valid = new Buffer(8).fill(0);
			valid.writeUInt32LE(0x000007FF);
			const qid = this._fidMap.get(msg.fid);
			console.error({fid: msg.fid, qid});
			// TODO
			const mode = STAT.IFDIR;
			const uid = 0;
			const gid = 0;
			const nlink = 1;
			const rdev = new Buffer(8).fill(0);
			const size = new Buffer(8).fill(0);
			const blksize = new Buffer(8).fill(0);
			// TODO
			blksize.writeUInt32LE(8 * 1024 * 1024);
			const blocks = new Buffer(8).fill(0);
			const atime_sec = new Buffer(8).fill(0);
			const atime_nsec = new Buffer(8).fill(0);
			const mtime_sec = new Buffer(8).fill(0);
			const mtime_nsec = new Buffer(8).fill(0);
			const ctime_sec = new Buffer(8).fill(0);
			const ctime_nsec = new Buffer(8).fill(0);
			const btime_sec = new Buffer(8).fill(0);
			const btime_nsec = new Buffer(8).fill(0);
			const gen = new Buffer(8).fill(0);
			const data_version = new Buffer(8).fill(0);

			this.replyOK(msg, {
				valid, qid, mode, uid, gid, nlink, rdev, size, blksize, blocks,
				atime_sec, atime_nsec, mtime_sec, mtime_nsec, ctime_sec,
				ctime_nsec, btime_sec, btime_nsec, gen, data_version});
		} else if(msg.type === Type.Tclunk) {
			this._fidMap.delete(msg.fid);
			this.replyOK(msg, {});
		} else if(msg.type === Type.Txattrwalk) {
			// We have no xattrs
			this.replyOK(msg, {size: new Buffer(8).fill(0)});
		} else if(msg.type === Type.Twalk) {
			const qid = this._fidMap.get(msg.fid);
			let parent = this._qidMap.get(_qid(qid).toString('hex'));
			const wqids = [];
			for(const wname of msg.wnames) {
				let row;
				try {
					row = yield terastash.getRowByParentBasename(
						this._client, this._stashName, parent, wname.toString('utf-8'), ['uuid', 'type']);
				} catch(err) {
					if(!(err instanceof terastash.NoSuchPathError)) {
						throw err;
					}
					break;
				}
				parent = row.uuid;
				const type = row.type === "f" ? "FILE" : "DIR";
				const qidPath = row.uuid.slice(0, 64/8); // UGH
				const qid = {type, version: 0, path: qidPath};
				this._qidMap.set(_qid(qid).toString('hex'), row.uuid);
				//console.error(`${inspect(wname)} -> ${inspect(qid)} -> ${inspect(row.uuid)}`);
				wqids.push(qid);
			}
			if(!msg.wnames.length) {
				this._fidMap.set(msg.newfid, this._fidMap.get(msg.fid));
			} else if(wqids.length) {
				this._fidMap.set(msg.newfid, wqids[wqids.length - 1]);
			}
			this.replyOK(msg, {wqids});
		} else if(msg.type === Type.Tlopen) {
			const qid = this._fidMap.get(msg.fid);
			const iounit = 8 * 1024 * 1024;
			this.replyOK(msg, {qid, iounit});
		} else if(msg.type === Type.Treaddir) {
			// TODO: support 64-bit offset
			// TODO: temporarily remember the rows and return more data for non-0 offset
			let rows = [];
			if(msg.offset.readUInt32LE() === 0) {
				const qid = this._fidMap.get(msg.fid);
				const parent = this._qidMap.get(_qid(qid).toString('hex'));
				T(parent, Buffer);
				rows = yield terastash.getChildrenForParent(
					this._client, this._stashName, parent,
					["basename", "type", "uuid"]
				);
			}
			const entries = [];
			let offset = 0;
			for(const row of rows) {
				const type = row.type === "f" ? "FILE" : "DIR";
				const qidPath = row.uuid.slice(0, 64/8); // UGH
				const qid = {type, version: 0, path: qidPath};
				this._qidMap.set(_qid(qid).toString('hex'), row.uuid);
				entries.push({qid, offset, type, name: new Buffer(row.basename, 'utf-8')});
				offset += 1;
			}
			this.replyOK(msg, {entries});
		} else {
			console.error("-> Unsupported message", msg);
			this.replyError(msg, "Unsupported message");
		}
	}

	*handleFrame(frameBuf) {
		const msg = decodeMessage(frameBuf);
		try {
			yield this.handleMessage(msg);
		} catch(err) {
			console.error(`Errored while processing ${inspect(msg)}:`);
			console.error(err.stack);
			this.replyError(msg, "Internal error");
		}
	}
}

Terastash9P.prototype.handleFrame = Promise.coroutine(Terastash9P.prototype.handleFrame);
Terastash9P.prototype.handleMessage = Promise.coroutine(Terastash9P.prototype.handleMessage);

function listen(socketPath) {
	T(socketPath, T.string);
	const server = net.createServer(function(client) {
		const ts = new Terastash9P(client);
		ts.init();
	});
	server.listen(socketPath);
	console.log(`9P server started, listening on UNIX domain socket at ${inspect(socketPath)}`);
}

module.exports = {listen};

// TODO: show correct type in directory listing
// TODO: give user all permissions
// TODO: make large directory listings work - stay under the msize
// TODO: implement file reads
