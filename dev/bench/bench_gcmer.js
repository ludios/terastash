"use strong";
"use strict";

const gcmer = require('../../gcmer');
const utils = require('../../utils');

const gcmWriter = new gcmer.GCMWriter(Number(process.argv[2]), new Buffer(16).fill(0), 0);
utils.pipeWithErrors(process.stdin, gcmWriter);
utils.pipeWithErrors(gcmWriter, process.stdout);
