Setup

  $ function nanos-now() { date -u +%s%N; } # Use nanos instead of seconds.nanos because bash can't do math on decimals
  $ export TERASTASH_COUNTERS_DIR="$(mktemp --tmpdir -d ts-test-d-state.XXXXXXXXXX)"
  $ nanos-now > "$TERASTASH_COUNTERS_DIR/start"
  $ mkdir -p /tmp/mychunks
  $ ts list-chunk-stores
  $ ts define-chunk-store mychunks -t localfs -d /tmp/mychunks -s '128*1024'
  $ ts destroy unit_tests_d > /dev/null 2>&1 || true # In case the last test run was ctrl-c'ed

Terastash works with -n option

  $ mkdir unit_tests_d
  $ cd unit_tests_d
  $ ts init unit_tests_d --chunk-store=mychunks
  $ echo hi > x
  $ touch --date=2015-01-01 x
  $ ts add x
  $ cd ..
  $ ts cat -n unit_tests_d x
  hi
  $ ts cat-ranges -n unit_tests_d x/0-3
  hi
  $ ts cat-ranges -n unit_tests_d x/1-3
  i
  $ ts get -n unit_tests_d x
  $ cat x
  hi
  $ ts export-db -n unit_tests_d
  {"~#Row":{"parent":"~bAAAAAAAAAAAAAAAAAAAAAA==","basename":"x","added_host":"test-hostname","added_time":"~t1970-01-01T00:00:00.000Z","added_user":"test-username","added_version":"test-version","block_size":null,"chunks_in_mychunks":null,"content":"~baGkK","crc32c":"~bG9ywgw==","executable":false,"key":null,"mtime":"~t2015-01-01T00:00:00.000Z","size":{"~#Long":"3"},"type":"f","uuid":"~bAAAAAAAAAAAAAAAAAAAAAQ==","version":3}}
  $ ts drop -n unit_tests_d x
  $ ts export-db -n unit_tests_d
  $ ts mkdir -n unit_tests_d sub dir
  $ ts ls -n unit_tests_d -j ""
  dir
  sub
  $ ts mv -n unit_tests_d sub dir
  $ ts ls -n unit_tests_d -j ""
  dir
  $ ts info -n unit_tests_d dir | grep -v '"mtime"'
  {
    "parent": "00000000000000000000000000000000",
    "basename": "dir",
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
    "size": null,
    "type": "d",
    "uuid": "00000000000000000000000000000003",
    "version": 3
  }
  $ ts info -n unit_tests_d no-such-file
  No entry with parent=00000000000000000000000000000000 and basename='no-such-file'
  [255]

End

  $ echo "$(($(nanos-now) - $(cat "$TERASTASH_COUNTERS_DIR/start")))" | sed -r 's/(.........)$/\.\1/g' > "$TERASTASH_COUNTERS_DIR/duration"
