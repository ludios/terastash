"use strong";
"use strict";

const hasher = require('./hasher');
const utils = require('./utils');

const crcReader = new hasher.CRCReader(Number(process.argv[2]));
utils.pipeWithErrors(process.stdin, crcReader);
utils.pipeWithErrors(crcReader, process.stdout);
