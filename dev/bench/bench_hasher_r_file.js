"use strict";

const hasher = require('../../hasher');
const utils = require('../../utils');
const fs = require('fs');

let crcReader;

crcReader = new hasher.CRCReader(Number(process.argv[3]));
utils.pipeWithErrors(fs.createReadStream(process.argv[2]), crcReader);
utils.pipeWithErrors(crcReader, process.stdout);

crcReader = new hasher.CRCReader(Number(process.argv[3]));
utils.pipeWithErrors(fs.createReadStream(process.argv[2]), crcReader);
utils.pipeWithErrors(crcReader, process.stdout);

crcReader = new hasher.CRCReader(Number(process.argv[3]));
utils.pipeWithErrors(fs.createReadStream(process.argv[2]), crcReader);
utils.pipeWithErrors(crcReader, process.stdout);

crcReader = new hasher.CRCReader(Number(process.argv[3]));
utils.pipeWithErrors(fs.createReadStream(process.argv[2]), crcReader);
utils.pipeWithErrors(crcReader, process.stdout);

crcReader = new hasher.CRCReader(Number(process.argv[3]));
utils.pipeWithErrors(fs.createReadStream(process.argv[2]), crcReader);
utils.pipeWithErrors(crcReader, process.stdout);
