"use strong";
"use strict";

const hasher = require('./hasher');
const utils = require('./utils');

const crcWriter = new hasher.CRCWriter(Number(process.argv[2]) * 1024);
utils.pipeWithErrors(process.stdin, crcWriter);
utils.pipeWithErrors(crcWriter, process.stdout);
