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



Prebuild removal feature
---

Some packages like leveldown use `prebuild`, which has a massive dependency
tree but is not actually needed if you're building the package yourself.  Simply
removing prebuild using `DEPS_BLACKLIST` is an incomplete solution: npm
still tries to run `prebuild` scripts mentioned in `package.json`, thus breaking
`npm install`.  So npm-hax also supports removing all 'scripts' in package.json
that start with `"prebuild "` with `REMOVE_PREBUILD=1`.  If you are versioning
node_modules/, this lets you avoid keeping prebuild's dependency tree around.
