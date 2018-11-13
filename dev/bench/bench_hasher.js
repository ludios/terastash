"use strict";

const hasher = require('../../hasher');
const utils = require('../../utils');

const crcWriter = new hasher.CRCWriter(Number(process.argv[2]));
utils.pipeWithErrors(process.stdin, crcWriter);
utils.pipeWithErrors(crcWriter, process.stdout);
