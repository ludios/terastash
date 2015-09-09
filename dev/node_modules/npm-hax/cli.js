#!/usr/bin/env node

"use strict";

const which = require('which');

const Module = require('module');
const _require = Module.prototype.require;
const _readJson = require('read-package-json');

function isObject(obj) {
	return obj && typeof obj === "object";
}

function readJson(file, log_, strict_, cb_) {
	let log, strict, cb;
	for(let i = 1; i < arguments.length - 1; i++) {
		if(typeof arguments[i] === 'boolean') {
			strict = arguments[i];
		} else if(typeof arguments[i] === 'function') {
			log = arguments[i];
		}
	}

	if (!log) {
		log = function () {};
	}
	cb = arguments[arguments.length - 1];

	// We replace cb with our own callback that modifies
	// data.{dependencies, devDependencies} first
	_readJson(file, log, strict, function(err, data) {
		// Don't serve fake deps when reading from ~/.npm; that
		// would be bad because it would affect projects that don't
		// want anything blacklisted.
		const inDotNpm = /[\/\\]\.npm[\/\\]/.test(file);

		const depsBlacklist = process.env.DEPS_BLACKLIST;
		if(!inDotNpm && isObject(data) && depsBlacklist) {
			for(const entry of depsBlacklist.split(' ')) {
				if(!entry) {
					continue;
				}
				const _ = entry.split('/');
				const depender = _[0];
				const dependee = _[1];
				if(depender === data.name) {
					if(isObject(data.dependencies)) {
						delete data.dependencies[dependee];
					}
					if(isObject(data.devDependencies)) {
						delete data.devDependencies[dependee];
					}
				}
			}
		}
		cb(err, data);
	});
}

// Patch require() to load our read-package-json replacement
Module.prototype.require = function cachePathsRequire(name) {
	if(name === 'read-package-json') {
		return readJson;
	}
	return _require.call(this, name);
}

require(which.sync('npm'));
