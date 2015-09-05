/**
 * WorkStealer is a simple stream that merely read()s from a source
 * and returns values when read from.  You can instantiate multiple
 * WorkStealer instances that read from the same object-mode stream,
 * therefore letting you parallelize the processing of some data.
 */

"use strong";
"use strict";

const T = require('notmytype');
const Readable = require('stream').Readable;

class WorkStealer extends Readable {
	constructor(inputStream) {
		T(inputStream, T.object);
		super({readableObjectMode: true});
		this._inputStream = inputStream;
		this._stopped = false;
		this._waiting = false;
	}

	_inputReadable() {
		if(this._stopped || !this._waiting) {
			return;
		}
		const obj = this._inputStream.read();
		if(obj === null) {
			return;
		}
		this._waiting = false;
		this.push(obj);
	}

	_stop() {
		if(this._stopped) {
			return;
		}
		this._stopped = true;
		this.push(null);
	}

	_inputError(err) {
		this.emit('error', err);
		this._stop();
	}

	_read() {
		const obj = this._inputStream.read();
		if(obj === null) {
			this._waiting = true;
		} else {
			this.push(obj);
		}
	}

	start() {
		this._inputStream.once('end', this._stop.bind(this));
		this._inputStream.once('error', this._inputError.bind(this));
		this._inputStream.on('readable', this._inputReadable.bind(this));
	}
}

function makeWorkStealers(inputStream, quantity) {
	const instances = [];
	while(quantity--) {
		instances.push(new WorkStealer(inputStream));
	}
	return instances;
}

module.exports = {WorkStealer, makeWorkStealers};
