npm-hax
===

[![NPM version][npm-image]][npm-url]

`npm-hax` is a replacement for the npm binary that lets you blacklist
dependencies at any depth in your dependency tree.  I was motivated to write
this after finding four different versions of [request](https://github.com/request/request)
in my dependency tree, which was noticeably slowing down my program's startup.

Don't use this unless you know what you're doing.  It's probably better to
submit pull requests bumping minor versions or changing them to lock to major versions.



Install
---

In your project, run:

```
npm install -g npm-hax
```

or install from the GitHub repo:

```
npm install -g ludios/npm-hax
```


Usage
---

The syntax is:

```
DEPS_BLACKLIST="module/dep ..." npm-hax any-npm-command
```

which will prevent npm from seeing `"dep": "version"` in both `dependencies`
and `devDependencies` in any `package.json` with `"name": "module"`.


Example
---

```sh
rm -rf node_modules
DEPS_BLACKLIST="googleapis/request google-auth-library/request \
gtoken/request gapitoken/request" npm-hax install
find node_modules/ | grep request
# victory dance
```

Remember to run `npm-hax` and not `npm`!!

[npm-image]: https://img.shields.io/npm/v/npm-hax.svg
[npm-url]: https://npmjs.org/package/npm-hax
