Setup

  $ function nanos-now() { date -u +%s%N; } # Use nanos instead of seconds.nanos because bash can't do math on decimals
  $ export TERASTASH_COUNTERS_DIR="$(mktemp --tmpdir -d ts-test-a-state.XXXXXXXXXX)"
  $ nanos-now > "$TERASTASH_COUNTERS_DIR/start"
  $ mkdir -p /tmp/mychunks
  $ ts list-chunk-stores
  $ ts define-chunk-store mychunks -t localfs -d /tmp/mychunks -s '80*1024'
  $ ts destroy unit_tests_a > /dev/null 2>&1 || true # In case the last test run was ctrl-c'ed

Continue

  $ ts ls
  File '([^\']+)' is not in a terastash working directory (re)
  [1]
  $ ts init unit_tests_a --chunk-store=mychunks

Can list stashes

  $ ts list-stashes | grep -P '^unit_tests_a$'
  unit_tests_a

Continue

  $ ts export-db # export should be empty for empty db
  $ ts list-chunk-stores
  mychunks
  $ ts get not-here
  No entry with parent=00000000000000000000000000000000 and basename='not-here'
  [1]
  $ ts get dir/not-here
  No entry with parent=00000000000000000000000000000000 and basename='dir'
  [1]
  $ echo -e "hello\nworld" > sample1
  $ touch --date=1970-01-01 sample1
  $ echo -e "second\nsample" > sample2
  $ touch --date=1980-01-01 sample2
  $ chmod +x sample2
  $ mkdir adir

Test for 'bytes instead of the expected' regression with 6MB file

  $ dd if=/dev/zero of=6MBfile bs=1000000 count=6 2> /dev/null
  $ md5sum 6MBfile
  75c6f06ec40f8063da34fcd7fc2bf17f  6MBfile
  $ ts add 6MBfile
  $ rm 6MBfile
  $ ts get 6MBfile
  $ md5sum 6MBfile
  75c6f06ec40f8063da34fcd7fc2bf17f  6MBfile
  $ rm 6MBfile
  $ ts drop 6MBfile

Continue

  $ dd bs=1024 count=1024 if=/dev/zero of=adir/bigfile 2> /dev/null
  $ touch --date=1995-01-01 adir
  $ cat adir/bigfile | md5sum | cut -f 1 -d " "
  b6d81b360a5672d80c27430f39153e2c
  $ touch --date=1990-01-01 adir/bigfile
  $ ts add sample1 sample2 adir/bigfile
  $ ts add sample1 # can't add again
  Cannot add to database: 'sample1' in stash 'unit_tests_a' already exists as a file
  [1]
  $ ts add -c sample1 # exit code 0
  Cannot add to database: 'sample1' in stash 'unit_tests_a' already exists as a file
  $ ts add -d sample1 # still the same, can't replace
  Cannot add to database: 'sample1' in stash 'unit_tests_a' already exists as a file
  [1]
  $ touch --date=1971-01-01 sample1
  $ ts add -d sample1 # different now, can replace
  Notice: replacing 'sample1' in db
  ┌───────┬─────────────────────────────────────────┬──────┬────────────┐
  │ which │ mtime                                   │ size │ executable │
  │ old   │ Thu Jan 01 1970 00:00:00 GMT+0000 (GMT) │ 12   │ false      │
  │ new   │ Fri Jan 01 1971 00:00:00 GMT+0000 (GMT) │ 12   │ false      │
  └───────┴─────────────────────────────────────────┴──────┴────────────┘
  $ mv adir adir.1
  $ touch adir
  $ ts add adir # can't add again
  Cannot add to database: 'adir' in stash 'unit_tests_a' already exists as a directory
  [1]
  $ rm adir
  $ mv adir.1 adir
  $ ts ls -n unit_tests_a
  When using -n/--name, a database path is required
  [1]
  $ ts ls
                   0 1995-01-01 00:00 adir/
                  12 1971-01-01 00:00 sample1
                  14 1980-01-01 00:00 sample2*
  $ ts ls -t
                   0 1995-01-01 00:00 adir/
                  14 1980-01-01 00:00 sample2*
                  12 1971-01-01 00:00 sample1
  $ ts ls -rt
                  12 1971-01-01 00:00 sample1
                  14 1980-01-01 00:00 sample2*
                   0 1995-01-01 00:00 adir/
  $ ts ls -j
  adir
  sample1
  sample2
  $ ts ls -rj
  sample2
  sample1
  adir
  $ ts ls -j -n unit_tests_a ''
  adir
  sample1
  sample2
  $ ts cat adir
  Path 'adir' in stash 'unit_tests_a' is not a file
  [1]
  $ ts cat sample1
  hello
  world
  $ ts cat sample1 sample2
  hello
  world
  second
  sample
  $ ts cat sample2 sample1
  second
  sample
  hello
  world
  $ rm -f sample1
  $ ls -1 sample1
  ls: cannot access sample1: No such file or directory
  [2]
  $ touch sample1 && chmod +x sample1 # Create an executable sample1 so that we can ensure permissions are reset
  $ ts get sample1
  $ ls -f sample1 # in output, no trailing '*', so it's not executable
  sample1
  $ stat -c %y sample1
  1971-01-01 00:00:00.000000000 +0000
  $ cat sample1
  hello
  world
  $ rm sample1 adir/bigfile
  $ ts cat adir/bigfile > adir/bigfile.copy
  $ cat adir/bigfile.copy | md5sum | cut -f 1 -d " "
  b6d81b360a5672d80c27430f39153e2c
  $ ts get sample1 adir/bigfile # Make sure 'ts get' works with > 1 file
  $ stat -c %y sample1
  1971-01-01 00:00:00.000000000 +0000
  $ stat -c %y adir/bigfile
  1990-01-01 00:00:00.000000000 +0000
  $ ts export-db > .export
  $ cat .export
  {"~#Row":{"parent":"~bAAAAAAAAAAAAAAAAAAAAAQ==","basename":"bigfile","block_size":8176,"chunks_in_mychunks":[{"idx":0,"file_id":"deterministic-filename-0","md5":null,"crc32c":"~bsd8r4g==","size":{"~#Long":"81920"}},{"idx":1,"file_id":"deterministic-filename-1","md5":null,"crc32c":"~b1CERyg==","size":{"~#Long":"81920"}},{"idx":2,"file_id":"deterministic-filename-2","md5":null,"crc32c":"~bd+YE0A==","size":{"~#Long":"81920"}},{"idx":3,"file_id":"deterministic-filename-3","md5":null,"crc32c":"~b0yVvzQ==","size":{"~#Long":"81920"}},{"idx":4,"file_id":"deterministic-filename-4","md5":null,"crc32c":"~bHl2NZw==","size":{"~#Long":"81920"}},{"idx":5,"file_id":"deterministic-filename-5","md5":null,"crc32c":"~bGHcYKA==","size":{"~#Long":"81920"}},{"idx":6,"file_id":"deterministic-filename-6","md5":null,"crc32c":"~b7pi+Pg==","size":{"~#Long":"81920"}},{"idx":7,"file_id":"deterministic-filename-7","md5":null,"crc32c":"~bepTbVA==","size":{"~#Long":"81920"}},{"idx":8,"file_id":"deterministic-filename-8","md5":null,"crc32c":"~brL7bGw==","size":{"~#Long":"81920"}},{"idx":9,"file_id":"deterministic-filename-9","md5":null,"crc32c":"~btfOj4g==","size":{"~#Long":"81920"}},{"idx":10,"file_id":"deterministic-filename-10","md5":null,"crc32c":"~bwDtB7Q==","size":{"~#Long":"81920"}},{"idx":11,"file_id":"deterministic-filename-11","md5":null,"crc32c":"~bt5VgLw==","size":{"~#Long":"81920"}},{"idx":12,"file_id":"deterministic-filename-12","md5":null,"crc32c":"~b5AiSBQ==","size":{"~#Long":"81920"}}],"content":null,"crc32c":null,"executable":false,"key":"~bAAAAAAAAAAAAAAAAAAAAAQ==","mtime":"~t1990-01-01T00:00:00.000Z","size":{"~#Long":"1048576"},"type":"f","uuid":null,"version":2}}
  {"~#Row":{"parent":"~bAAAAAAAAAAAAAAAAAAAAAA==","basename":"adir","block_size":null,"chunks_in_mychunks":null,"content":null,"crc32c":null,"executable":null,"key":null,"mtime":"~t1995-01-01T00:00:00.000Z","size":null,"type":"d","uuid":"~bAAAAAAAAAAAAAAAAAAAAAQ==","version":2}}
  {"~#Row":{"parent":"~bAAAAAAAAAAAAAAAAAAAAAA==","basename":"sample1","block_size":null,"chunks_in_mychunks":null,"content":"~baGVsbG8Kd29ybGQK","crc32c":"~bU49V7A==","executable":false,"key":null,"mtime":"~t1971-01-01T00:00:00.000Z","size":{"~#Long":"12"},"type":"f","uuid":null,"version":2}}
  {"~#Row":{"parent":"~bAAAAAAAAAAAAAAAAAAAAAA==","basename":"sample2","block_size":null,"chunks_in_mychunks":null,"content":"~bc2Vjb25kCnNhbXBsZQo=","crc32c":"~bNos/Jg==","executable":true,"key":null,"mtime":"~t1980-01-01T00:00:00.000Z","size":{"~#Long":"14"},"type":"f","uuid":null,"version":2}}
  $ ts destroy unit_tests_a # destroy before importing the .export we made
  Destroyed keyspace and removed config for unit_tests_a.

Stash is not listed after being destroyed

  $ ts list-stashes | grep -P '^unit_tests_a$'
  [1]

Continue

  $ ts init unit_tests_a --chunk-store=mychunks
  $ ts import-db -n unit_tests_a .export
  $ ts export-db > .export-again
  $ diff -u .export .export-again # db should be identical after export, destroy, import
  $ ts drop adir # Can't drop a non-empty directory
  Refusing to drop 'adir' because it is a non-empty directory
  [1]
  $ ts drop sample1 adir/bigfile adir
  $ ts ls
                  14 1980-01-01 00:00 sample2*
  $ ls -1F sample2
  sample2*
  $ rm sample2
  $ ts get sample2
  $ ls -1F sample2
  sample2*
  $ ts drop sample2
  $ ts ls

Parent directories are automatically created as needed

  $ mkdir -p d1/d2/d3
  $ touch d1/d2/d3/empty
  $ ts add d1/d2/d3/empty
  $ ts ls -j d1
  d2
  $ ts ls -j d1/d2
  d3
  $ ts ls -j d1/d2/d3
  empty
  $ ts mkdir d1/d2/d3 # make sure this doesn't overwrite existing dir
  $ ts ls -j d1/d2/d3
  empty
  $ ts drop d1/d2/d3/empty d1/d2/d3 d1/d2 d1

Dropping file again throws an error

  $ ts drop sample1
  No entry with parent=00000000000000000000000000000000 and basename='sample1'
  [1]
  $ ts ls

Dropping nonexistent file throws an error

  $ ts drop doesntexist
  No entry with parent=00000000000000000000000000000000 and basename='doesntexist'
  [1]
  $ ts ls

End

  $ echo "$(($(nanos-now) - $(cat "$TERASTASH_COUNTERS_DIR/start")))" | sed -r 's/(.........)$/\.\1/g' > "$TERASTASH_COUNTERS_DIR/duration"
