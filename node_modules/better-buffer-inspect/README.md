better-buffer-inspect
===

[![NPM version][npm-image]][npm-url]
[![Build status][travis-image]][travis-url]

better-buffer-inspect makes `util.inspect(aBuffer)` show the size of
an io.js Buffer, as well as its text content if it is <= 512 bytes and
decodes as UTF-8.


Install
---

In your project, run:

```
npm install better-buffer-inspect --save
```

or install from the GitHub repo:

```
npm install ludios/better-buffer-inspect --save
```


API
---
Requiring `better-buffer-inspect` will override `Buffer.prototype.inspect`.

```js
> require('better-buffer-inspect');

> new Buffer(4).fill(255)
<Buffer size=4: ff ff ff ff>

> new Buffer("hello world\n")
<Buffer size=11: utf8 'hello world\n'>

> new Buffer("\ucccc")
<Buffer size=3: utf8 '쳌'>
```


[npm-image]: https://img.shields.io/npm/v/better-buffer-inspect.svg
[npm-url]: https://npmjs.org/package/better-buffer-inspect
[travis-image]: https://img.shields.io/travis/ludios/better-buffer-inspect.svg
[travis-url]: https://travis-ci.org/ludios/better-buffer-inspect
