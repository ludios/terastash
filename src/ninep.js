"use strict";

const A         = require('ayy');
const T         = require('notmytype');
const Promise   = require('bluebird');
const net       = require('net');
const inspect   = require('util').inspect;
const utils     = require('./utils');
const terastash = require('./');
const intreader = require('intreader');
const chalk     = require('chalk');
const cassandra = require('cassandra-driver');

const DEBUG_9P = Boolean(Number(process.env.TERASTASH_DEBUG_9P));

function _buf(size) {
	return function(buf) {
		A.eq(buf.length, size);
		return buf;
	};
}

/* 9P2000.L types */
const QT = {
	DIR:    0x80, // directory
	APPEND: 0x40, // append-only file
	EXCL:   0x20, // exclusive use file
	MOUNT:  0x10, // mounted channel
	AUTH:   0x08, // authentication file
	TMP:    0x04, // non-backed-up file
	LINK:   0x02, // symbolic link
	FILE:   0x00 // regular file
};

/* VFS types from dirent.h */
const DT = {
	 UNKNOWN: 0
	,FIFO:    1
	,CHR:     2
	,DIR:     4
	,BLK:     6
	,REG:     8
	,LNK:     10
	,SOCK:    12
	,WHT:     14
};

/* stat mode bits from http://osxr.org/glibc/source/sysdeps/unix/sysv/linux/x86/bits/stat.h#0182 */
const STAT = {
	 IFDIR:  0o0040000 /* Directory */
	,IFCHR:  0o0020000 /* Character device */
	,IFBLK:  0o0060000 /* Block device */
	,IFREG:  0o0100000 /* Regular file */
	,IFIFO:  0o0010000 /* FIFO */
	,IFLNK:  0o0120000 /* Symbolic link */
	,IFSOCK: 0o0140000 /* Socket */
};

//console.error({fidMap: this._fidMap, qidMap: this._qidMap});

function _enc_Rwalk(obj) {
	const wqids = obj.wqids;
	return [uint16(wqids.length)].concat(wqids.map(_qid));
}

function _enc_Rread(obj) {
	return [uint32(obj.data.length), obj.data];
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

const packets = {
	// https://github.com/chaos/diod/blob/master/protocol.md
	// http://lxr.free-electrons.com/source/include/net/9p/9p.h
	8: {name: "Tstatfs"},
	9: {name: "Rstatfs"},
	12: {name: "Tlopen"},
	13: {name: "Rlopen", enc: ["qid", _qid, "iounit", uint32]},
	14: {name: "Tlcreate"},
	15: {name: "Rlcreate"},
	24: {name: "Tgetattr"},
	25: {name: "Rgetattr", enc: [
		"valid",        _buf(8),
		"qid",          _qid,
		"mode",         uint32,
		"uid",          uint32,
		"gid",          uint32,
		"nlink",        uint64,
		"rdev",         _buf(8),
		"size",         uint64,
		"blksize",      _buf(8),
		"blocks",       _buf(8),
		"atime_sec",    _buf(8),
		"atime_nsec",   _buf(8),
		"mtime_sec",    _buf(8),
		"mtime_nsec",   _buf(8),
		"ctime_sec",    _buf(8),
		"ctime_nsec",   _buf(8),
		"btime_sec",    _buf(8),
		"btime_nsec",   _buf(8),
		"gen",          _buf(8),
		"data_version", _buf(8)
	]},
	26: {name: "Tsetattr"},
	27: {name: "Rsetattr"},
	30: {name: "Txattrwalk"},
	31: {name: "Rxattrwalk", enc: ["size", _buf(8)]},
	40: {name: "Treaddir"},
	41: {name: "Rreaddir", enc: _enc_Rreaddir},
	50: {name: "Tfsync"},
	51: {name: "Rfsync"},
	72: {name: "Tmkdir"},
	73: {name: "Rmkdir"},
	100: {name: "Tversion"},
	101: {name: "Rversion", enc: ["msize", uint32, "version", string]},
	102: {name: "Tauth"},
	103: {name: "Rauth"},
	104: {name: "Tattach"},
	105: {name: "Rattach", enc: ["qid", _qid]},
	107: {name: "Rerror"},
	108: {name: "Tflush"},
	109: {name: "Rflush"},
	110: {name: "Twalk"},
	111: {name: "Rwalk", enc: _enc_Rwalk},
	112: {name: "Topen"},
	113: {name: "Ropen"},
	114: {name: "Tcreate"},
	115: {name: "Rcreate"},
	116: {name: "Tread"},
	117: {name: "Rread", enc: _enc_Rread},
	118: {name: "Twrite"},
	119: {name: "Rwrite"},
	120: {name: "Tclunk"},
	121: {name: "Rclunk", enc: []},
	122: {name: "Tremove"},
	124: {name: "Tstat"},
	125: {name: "Rstat"},
	126: {name: "Twstat"}
};

const Type = {};
for(const p of Object.keys(packets)) {
	Type[packets[p].name] = Number(p);
}

const LongLikeType = T.shape({
	low: T.number,
	high: T.number,
	unsigned: T.boolean
});

function uint64(n) {
	T(n, T.union([T.number, LongLikeType]));
	const buf = Buffer.alloc(8);
	if(typeof n === 'number') {
		A.gte(n, 0);
		if(n <= Math.pow(2, 32)) {
			buf.writeUInt32LE(n);
			return buf;
		} else {
			n = cassandra.types.Long.fromNumber(n, true);
		}
	}
	// This *should* be true, but cassandra-driver is not setting it?
	//A.eq(n.unsigned, true);
	buf.writeInt32LE(n.low);
	buf.writeInt32LE(n.high, 4);
	return buf;
}

function uint64BufferToNumber(buf) {
	const low = buf.readUInt32LE(0);
	const high = buf.readUInt32LE(4);
	return high * Math.pow(2, 32) + low;
}
A.eq(uint64BufferToNumber(uint64(Math.pow(2, 40) + 10)), Math.pow(2, 40) + 10);

function uint32(n) {
	T(n, T.number);
	A.gte(n, 0);
	A.lte(n, Math.pow(2, 32));
	const buf = Buffer.allocUnsafe(4);
	buf.writeUInt32LE(n, 0);
	return buf;
}

function uint16(n) {
	T(n, T.number);
	A.gte(n, 0);
	A.lte(n, Math.pow(2, 16));
	const buf = Buffer.allocUnsafe(2);
	buf.writeUInt16LE(n, 0);
	return buf;
}

function uint8(n) {
	T(n, T.number);
	A.gte(n, 0);
	A.lte(n, Math.pow(2, 8));
	const buf = Buffer.allocUnsafe(1);
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
		const fid          = frame.uint32();
		const offset       = frame.buffer(8);
		const count        = frame.uint32();
		return {type, tag, fid, offset, count};
	} else if(type === Type.Twrite) {
		const fid          = frame.uint32();
		const offset       = frame.buffer(8);
		const count        = frame.uint32();
		const data         = frame.buffer(count);
		return {type, tag, fid, offset, data};
	} else if(type === Type.Treaddir) {
		const fid          = frame.uint32();
		const offset       = frame.buffer(8);
		const count        = frame.uint32();
		return {type, tag, fid, offset, count};
	} else if(type === Type.Tversion) {
		const msize        = frame.uint32();
		const version      = frame.string();
		return {type, tag, msize, version};
	} else if(type === Type.Tattach) {
		const fid          = frame.uint32();
		const afid         = frame.uint32();
		const uname        = frame.string();
		const aname        = frame.string();
		return {type, tag, fid, afid, uname, aname};
	} else if(type === Type.Tgetattr) {
		const fid          = frame.uint32();
		const request_mask = frame.buffer(8);
		return {type, tag, fid, request_mask};
	} else if(type === Type.Tclunk) {
		const fid          = frame.uint32();
		return {type, tag, fid};
	} else if(type === Type.Txattrwalk) {
		const fid          = frame.uint32();
		const newfid       = frame.uint32();
		const name         = frame.string();
		return {type, tag, fid, newfid, name};
	} else if(type === Type.Twalk) {
		const fid          = frame.uint32();
		const newfid       = frame.uint32();
		const nwname       = frame.uint16();
		const wnames       = [];
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
		const oldtag = frame.uint16();
		return {type, tag, oldtag};
	} else {
		return {type, tag, decode_error: "Unsupported message"};
	}
}


class Terastash9P {
	constructor(peer) {
		this._peer = peer;
		this._stashInfo = null;
		this._qidMap = new Map();
		this._fidMap = new Map();
		this._ourMax = (64 * 1024 * 1024) - 4;
		this._msize = null;
		this._client = terastash.getNewClient();
		this._myUID = process.getuid();
	}

	init() {
		const decoder = new intreader.Int32Reader("LE", this._ourMax, true);
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
		const preBuf = Buffer.allocUnsafe(4 + 1 + 2);
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
		if(DEBUG_9P) {
			console.error(chalk.red(`<- ${packets[type].name}\n${inspect(Object.assign(obj, {tag}))}`));
		}

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
		if(DEBUG_9P) {
			console.error(chalk.bold(chalk.red(`<- ${packets[type].name} ${inspect(reason)}`)));
		}
		this.replyAny(msg.tag, type, [string(Buffer.from(reason))]);
	}

	*handleMessage(msg) {
		if(DEBUG_9P) {
			console.error(chalk.cyan(`-> ${(packets[String(msg.type)] || {name: "?"}).name}\n${inspect(msg)}`));
		}
		if(msg.type === Type.Tversion) {
			// TODO: ensure version is 9P2000.L
			// http://man.cat-v.org/plan_9/5/version - we must respond
			// with an equal or smaller msize.  Note that msize includes
			// the size int itself.
			this._msize = Math.min(msg.msize, this._ourMax + 4);
			this.replyOK(msg, {msize: this._msize, version: msg.version});
		} else if(msg.type === Type.Tattach) {
			const stashName = msg.aname.toString('utf-8');
			this._stashInfo = yield terastash.getStashInfoByName(stashName);
			const qid = {type: "DIR", version: 0, path: Buffer.alloc(8)};
			// UUID 0000... is the root of the stash
			this._qidMap.set(_qid(qid).toString('hex'), {uuid: Buffer.alloc(128/8), type: "DIR", executable: false, size: 0});
			this._fidMap.set(msg.fid, qid);
			this.replyOK(msg, {qid});
		} else if(msg.type === Type.Tgetattr) {
			const valid = Buffer.alloc(8);
			valid.writeUInt32LE(0x000007FF);
			const qid = this._fidMap.get(msg.fid);
			let {type, uuid, executable, size} = this._qidMap.get(_qid(qid).toString('hex'));
			if(DEBUG_9P) {
				console.error({fid: msg.fid, qid});
			}
			let mode =
				type === "DIR" ?
					STAT.IFDIR | 0o770 :
					STAT.IFREG | 0o660;
			if(executable) {
				mode |= 0o770;
			}
			const uid = this._myUID;
			const gid = 0;
			const nlink = 1;
			const rdev = Buffer.alloc(8);
			if(type === "DIR") {
				size = 0;
			} else {
				A.neq(size, null, `Size for qid ${inspect(qid)} was null; type=${inspect(type)}`);
			}
			const blksize = Buffer.alloc(8);
			// TODO
			blksize.writeUInt32LE(8 * 1024 * 1024);
			const blocks = Buffer.alloc(8);
			const atime_sec = Buffer.alloc(8);
			const atime_nsec = Buffer.alloc(8);
			// TODO
			const mtime_sec = Buffer.alloc(8);
			const mtime_nsec = Buffer.alloc(8);
			const ctime_sec = Buffer.alloc(8);
			const ctime_nsec = Buffer.alloc(8);
			const btime_sec = Buffer.alloc(8);
			const btime_nsec = Buffer.alloc(8);
			const gen = Buffer.alloc(8);
			const data_version = Buffer.alloc(8);

			this.replyOK(msg, {
				valid, qid, mode, uid, gid, nlink, rdev, size, blksize, blocks,
				atime_sec, atime_nsec, mtime_sec, mtime_nsec, ctime_sec,
				ctime_nsec, btime_sec, btime_nsec, gen, data_version});
		} else if(msg.type === Type.Tread) {
			const qid = this._fidMap.get(msg.fid);
			const {parent, basename} = this._qidMap.get(_qid(qid).toString('hex'));
			const offset = uint64BufferToNumber(msg.offset);
			const count = msg.count;
			const [row, readStream] = yield terastash.streamFile(this._client, this._stashInfo, parent, basename, [[offset, offset + count]]);
			const data = yield utils.readableToBuffer(readStream);
			this.replyOK(msg, {data});
		} else if(msg.type === Type.Tclunk) {
			this._fidMap.delete(msg.fid);
			this.replyOK(msg, {});
		} else if(msg.type === Type.Txattrwalk) {
			// We have no xattrs
			this.replyOK(msg, {size: Buffer.alloc(8)});
		} else if(msg.type === Type.Twalk) {
			const qid = this._fidMap.get(msg.fid);
			let parent = this._qidMap.get(_qid(qid).toString('hex')).uuid;
			const wqids = [];
			for(const wname of msg.wnames) {
				let row;
				try {
					row = yield terastash.getRowByParentBasename(
						this._client, this._stashInfo.name, parent, wname.toString('utf-8'), ['uuid', 'type', 'executable', 'parent', 'basename', 'size']);
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
				this._qidMap.set(_qid(qid).toString('hex'), {
					uuid: row.uuid, type: type, executable: row.executable, parent: row.parent, basename: row.basename, size: row.size});
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
				const parent = this._qidMap.get(_qid(qid).toString('hex')).uuid;
				T(parent, Buffer);
				rows = yield terastash.getChildrenForParent(
					this._client, this._stashInfo.name, parent,
					["basename", "type", "uuid", "parent", "size", "executable"]
				);
			}
			const entries = [];
			let offset = 1;
			for(const row of rows) {
				const type = row.type === "f" ? "FILE" : "DIR";
				const qidPath = row.uuid.slice(0, 64/8); // UGH
				const qid = {type, version: 0, path: qidPath};
				this._qidMap.set(_qid(qid).toString('hex'), {
					uuid: row.uuid, type: type, executable: row.executable, parent: row.parent, basename: row.basename, size: row.size});
				entries.push({qid, offset, type, name: Buffer.from(row.basename, 'utf-8')});
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

// TODO: test how many unfulfilled reads that Linux kernel issues
	// Some testing showed that it keeps ~432 reads going, with an msize of 65536
	// Tested 4.3-rc3 kernel - saw 997 reads, perhaps no limit
// TODO: handle Tflush
// TODO: don't get streamFile a range larger than the actual file?  Hitting this assert:
//Error: For parent=16fc2b528c139e05221253971c218412 basename='Torrent downloaded from Demonoid.me.txt', expected length of content to be
//8,192 but was
//46
//    at streamFile$end (/mnt/devdrive/NodeProjects/terastash/index.js:1498:29)
// TODO: show correct mtime for a file
// TODO: make large directory listings work - stay under the msize
// TODO: implement readahead - if we keep getting subsequent reads on a file, keep doubling the request size up to a maximum
//	Fulfill read as soon as possible even while we wait for the rest to arrive into the cache
