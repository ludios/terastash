"use strong";
"use strict";

const hasher = require('../../hasher');
const utils = require('../../utils');

const crcWriter = new hasher.CRCWriter(Number(process.argv[2]));
const crcReader = new hasher.CRCReader(Number(process.argv[2]));
utils.pipeWithErrors(process.stdin, crcWriter);
utils.pipeWithErrors(crcWriter, crcReader);
utils.pipeWithErrors(crcReader, process.stdout);
