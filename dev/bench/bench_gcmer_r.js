"use strong";
"use strict";

const gcmer = require('../../gcmer');
const utils = require('../../utils');

const gcmReader = new gcmer.GCMReader(Number(process.argv[2]), new Buffer(16).fill(0), 0);
utils.pipeWithErrors(process.stdin, gcmReader);
utils.pipeWithErrors(gcmReader, process.stdout);
