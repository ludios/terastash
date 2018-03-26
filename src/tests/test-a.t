Setup

  $ function nanos-now() { date -u +%s%N; } # Use nanos instead of seconds.nanos because bash can't do math on decimals
  $ export TERASTASH_COUNTERS_DIR="$(mktemp --tmpdir -d ts-test-a-state.XXXXXXXXXX)"
  $ nanos-now > "$TERASTASH_COUNTERS_DIR/start"
  $ mkdir -p /tmp/mychunks
  $ ts list-chunk-stores
  $ ts define-chunk-store mychunks -t localfs -d /tmp/mychunks -s '128*1024'
  $ ts destroy unit_tests_a > /dev/null 2>&1 || true # In case the last test run was ctrl-c'ed

Continue

  $ ts ls
  File '([^\']+)' is not in a terastash working directory (re)
  [255]
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
  [255]
  $ ts get dir/not-here
  No entry with parent=00000000000000000000000000000000 and basename='dir'
  [255]
  $ echo -e "hello\nworld" > sample1
  $ touch --date=1970-01-01 sample1
  $ echo -e "second\nsample" > sample2
  $ touch --date=1980-01-01 sample2
  $ chmod +x sample2
  $ mkdir adir

Test for 'bytes instead of the expected' regression with 6MB file, also cat-ranges

  $ dd if=/dev/zero of=6MBfile bs=1000000 count=6 2> /dev/null
  $ md5sum 6MBfile
  75c6f06ec40f8063da34fcd7fc2bf17f  6MBfile
  $ ts add 6MBfile
  $ rm 6MBfile
  $ ts get 6MBfile
  $ md5sum 6MBfile
  75c6f06ec40f8063da34fcd7fc2bf17f  6MBfile
  $ rm 6MBfile
  $ ts cat-ranges 6MBfile/0-1000000 6MBfile/1000000-2000000 6MBfile/2000000-3000000 6MBfile/3000000-4000000 6MBfile/4000000-5000000 6MBfile/5000000-6000000 | md5sum
  75c6f06ec40f8063da34fcd7fc2bf17f  -
  $ ts cat-ranges 6MBfile/1-1000000 6MBfile/1000000-2000000 6MBfile/2000000-3000000 6MBfile/3000000-4000000 6MBfile/4000000-5000000 6MBfile/5000000-6000000 | md5sum
  27cbf3e7a79ab50c4e6b982aa03d7bed  -
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
  [255]
  $ ts add -c sample1 # exit code 0
  Cannot add to database: 'sample1' in stash 'unit_tests_a' already exists as a file
  $ ts add -d sample1 # still the same, can't replace
  Cannot add to database: 'sample1' in stash 'unit_tests_a' already exists as a file
  [255]
  $ touch --date=1971-01-01 sample1
  $ ts add --ignore-mtime -d sample1
  Cannot add to database: 'sample1' in stash 'unit_tests_a' already exists as a file
  [255]
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
  [255]
  $ rm adir
  $ mv adir.1 adir
  $ ts ls -n unit_tests_a
  When using -n/--name, a database path is required
  [255]
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
  $ ts ls -S
                  14 1980-01-01 00:00 sample2*
                  12 1971-01-01 00:00 sample1
                   0 1995-01-01 00:00 adir/
  $ ts ls -Sr
                   0 1995-01-01 00:00 adir/
                  12 1971-01-01 00:00 sample1
                  14 1980-01-01 00:00 sample2*
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
  Object parent=00000000000000000000000000000000 basename='adir' in stash 'unit_tests_a' is not a file; got type 'd'
  [255]
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
  ls: cannot access 'sample1': No such file or directory
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
  $ ts info sample1 adir adir/bigfile
  {
    "parent": "00000000000000000000000000000000",
    "basename": "sample1",
    "added_host": "test-hostname",
    "added_time": "1970-01-01T00:00:00.000Z",
    "added_user": "test-username",
    "added_version": "test-version",
    "block_size": null,
    "chunks_in_mychunks": null,
    "content": "68656c6c6f0a776f726c640a",
    "crc32c": "538f55ec",
    "executable": false,
    "key": null,
    "mtime": "1971-01-01T00:00:00.000Z",
    "size": 12,
    "type": "f",
    "uuid": "00000000000000000000000000000006",
    "version": 3
  }
  {
    "parent": "00000000000000000000000000000000",
    "basename": "adir",
    "added_host": "test-hostname",
    "added_time": "1970-01-01T00:00:00.000Z",
    "added_user": "test-username",
    "added_version": "test-version",
    "block_size": null,
    "chunks_in_mychunks": null,
    "content": null,
    "crc32c": null,
    "executable": null,
    "key": null,
    "mtime": "1995-01-01T00:00:00.000Z",
    "size": null,
    "type": "d",
    "uuid": "00000000000000000000000000000005",
    "version": 3
  }
  {
    "parent": "00000000000000000000000000000005",
    "basename": "bigfile",
    "added_host": "test-hostname",
    "added_time": "1970-01-01T00:00:00.000Z",
    "added_user": "test-username",
    "added_version": "test-version",
    "block_size": 65520,
    "chunks_in_mychunks": [
      {
        "idx": 0,
        "file_id": "deterministic-filename-0",
        "md5": null,
        "crc32c": "65da012d",
        "size": 131072,
        "account": null
      },
      {
        "idx": 1,
        "file_id": "deterministic-filename-1",
        "md5": null,
        "crc32c": "eb665297",
        "size": 131072,
        "account": null
      },
      {
        "idx": 2,
        "file_id": "deterministic-filename-2",
        "md5": null,
        "crc32c": "0f222689",
        "size": 131072,
        "account": null
      },
      {
        "idx": 3,
        "file_id": "deterministic-filename-3",
        "md5": null,
        "crc32c": "ad10596f",
        "size": 131072,
        "account": null
      },
      {
        "idx": 4,
        "file_id": "deterministic-filename-4",
        "md5": null,
        "crc32c": "30a7c6c6",
        "size": 131072,
        "account": null
      },
      {
        "idx": 5,
        "file_id": "deterministic-filename-5",
        "md5": null,
        "crc32c": "5d732ec0",
        "size": 131072,
        "account": null
      },
      {
        "idx": 6,
        "file_id": "deterministic-filename-6",
        "md5": null,
        "crc32c": "1821bd54",
        "size": 131072,
        "account": null
      },
      {
        "idx": 7,
        "file_id": "deterministic-filename-7",
        "md5": null,
        "crc32c": "270166e0",
        "size": 131072,
        "account": null
      },
      {
        "idx": 8,
        "file_id": "deterministic-filename-8",
        "md5": null,
        "crc32c": "ddb55358",
        "size": 16384,
        "account": null
      }
    ],
    "content": null,
    "crc32c": null,
    "executable": false,
    "key": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "mtime": "1990-01-01T00:00:00.000Z",
    "size": 1048576,
    "type": "f",
    "uuid": "00000000000000000000000000000004",
    "version": 3
  }
  $ ts info -k adir/bigfile | grep '"key"'
    "key": "00000000000000000000000000000001",
  $ ts export-db | sort > .export
  $ cat .export
  {"~#Row":{"parent":"~bAAAAAAAAAAAAAAAAAAAAAA==","basename":"adir","added_host":"test-hostname","added_time":"~t1970-01-01T00:00:00.000Z","added_user":"test-username","added_version":"test-version","block_size":null,"chunks_in_mychunks":null,"content":null,"crc32c":null,"executable":null,"key":null,"mtime":"~t1995-01-01T00:00:00.000Z","size":null,"type":"d","uuid":"~bAAAAAAAAAAAAAAAAAAAABQ==","version":3}}
  {"~#Row":{"parent":"~bAAAAAAAAAAAAAAAAAAAAAA==","basename":"sample1","added_host":"test-hostname","added_time":"~t1970-01-01T00:00:00.000Z","added_user":"test-username","added_version":"test-version","block_size":null,"chunks_in_mychunks":null,"content":"~baGVsbG8Kd29ybGQK","crc32c":"~bU49V7A==","executable":false,"key":null,"mtime":"~t1971-01-01T00:00:00.000Z","size":{"~#Long":"12"},"type":"f","uuid":"~bAAAAAAAAAAAAAAAAAAAABg==","version":3}}
  {"~#Row":{"parent":"~bAAAAAAAAAAAAAAAAAAAAAA==","basename":"sample2","added_host":"test-hostname","added_time":"~t1970-01-01T00:00:00.000Z","added_user":"test-username","added_version":"test-version","block_size":null,"chunks_in_mychunks":null,"content":"~bc2Vjb25kCnNhbXBsZQo=","crc32c":"~bNos/Jg==","executable":true,"key":null,"mtime":"~t1980-01-01T00:00:00.000Z","size":{"~#Long":"14"},"type":"f","uuid":"~bAAAAAAAAAAAAAAAAAAAAAw==","version":3}}
  {"~#Row":{"parent":"~bAAAAAAAAAAAAAAAAAAAABQ==","basename":"bigfile","added_host":"test-hostname","added_time":"~t1970-01-01T00:00:00.000Z","added_user":"test-username","added_version":"test-version","block_size":65520,"chunks_in_mychunks":[{"idx":0,"file_id":"deterministic-filename-0","md5":null,"crc32c":"~bZdoBLQ==","size":{"~#Long":"131072"},"account":null},{"idx":1,"file_id":"deterministic-filename-1","md5":null,"crc32c":"~b62ZSlw==","size":{"~#Long":"131072"},"account":null},{"idx":2,"file_id":"deterministic-filename-2","md5":null,"crc32c":"~bDyImiQ==","size":{"~#Long":"131072"},"account":null},{"idx":3,"file_id":"deterministic-filename-3","md5":null,"crc32c":"~brRBZbw==","size":{"~#Long":"131072"},"account":null},{"idx":4,"file_id":"deterministic-filename-4","md5":null,"crc32c":"~bMKfGxg==","size":{"~#Long":"131072"},"account":null},{"idx":5,"file_id":"deterministic-filename-5","md5":null,"crc32c":"~bXXMuwA==","size":{"~#Long":"131072"},"account":null},{"idx":6,"file_id":"deterministic-filename-6","md5":null,"crc32c":"~bGCG9VA==","size":{"~#Long":"131072"},"account":null},{"idx":7,"file_id":"deterministic-filename-7","md5":null,"crc32c":"~bJwFm4A==","size":{"~#Long":"131072"},"account":null},{"idx":8,"file_id":"deterministic-filename-8","md5":null,"crc32c":"~b3bVTWA==","size":{"~#Long":"16384"},"account":null}],"content":null,"crc32c":null,"executable":false,"key":"~bAAAAAAAAAAAAAAAAAAAAAQ==","mtime":"~t1990-01-01T00:00:00.000Z","size":{"~#Long":"1048576"},"type":"f","uuid":"~bAAAAAAAAAAAAAAAAAAAABA==","version":3}}
  $ ts destroy unit_tests_a # destroy before importing the .export we made
  Destroyed keyspace and removed config for unit_tests_a.

Stash is not listed after being destroyed

  $ ts list-stashes | grep -P '^unit_tests_a$'
  [1]

Continue

  $ ts init unit_tests_a --chunk-store=mychunks

Add a file with size > 4GB to test for Long decoding regression

  $ touch /tmp/mychunks/deterministic-filename-FAKEFAKE # need a file for 'ts drop' to unlink later
  $ echo '{"~#Row":{"parent":"~bAAAAAAAAAAAAAAAAAAAABQ==","basename":"zz_over4gb","added_host":"test-hostname","added_time":"~t1970-01-01T00:00:00.000Z","added_user":"test-username","added_version":"test-version","block_size":65520,"chunks_in_mychunks":[{"idx":0,"file_id":"deterministic-filename-FAKEFAKE","md5":null,"crc32c":"~bXXMuwA==","size":{"~#Long":"7717519360"},"account":null}],"content":null,"crc32c":null,"executable":false,"key":"~bAAAAAAAAAAAAAAAAAAAAAQ==","mtime":"~t1990-01-01T00:00:00.000Z","size":{"~#Long":"7681243620"},"type":"f","uuid":"~bAAAAAAAAAAAAAAAAAAAAzw==","version":3}}' >> .export
  $ cat .export | sort > .export-sorted && mv .export-sorted .export

Continue

  $ ts import-db -n unit_tests_a .export
  $ ts export-db | sort > .export-again
  $ diff -u .export .export-again # db should be identical after export, destroy, import
  $ ts drop adir # Can't drop a non-empty directory
  Refusing to drop 'adir' because it is a non-empty directory
  [255]
  $ ts drop sample1 adir/zz_over4gb adir/bigfile adir
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
  [255]
  $ ts ls

Dropping nonexistent file throws an error

  $ ts drop doesntexist
  No entry with parent=00000000000000000000000000000000 and basename='doesntexist'
  [255]
  $ ts ls

Test cat-ranges on file whose last chunk is not divisible by blockSize

  $ dd if=/dev/zero of=32KBfile bs=1024 count=32 2> /dev/null
  $ ts add 32KBfile
  $ ts cat-ranges 32KBfile/0-100 | md5sum
  6d0bb00954ceb7fbee436bb55a8397a9  -
  $ ts cat-ranges 32KBfile/100-200 | md5sum
  6d0bb00954ceb7fbee436bb55a8397a9  -
  $ ts cat-ranges 32KBfile/32668-32768 | md5sum
  6d0bb00954ceb7fbee436bb55a8397a9  -

End

  $ echo "$(($(nanos-now) - $(cat "$TERASTASH_COUNTERS_DIR/start")))" | sed -r 's/(.........)$/\.\1/g' > "$TERASTASH_COUNTERS_DIR/duration"
