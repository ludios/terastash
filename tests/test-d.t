Setup

  $ function nanos-now() { date -u +%s%N; } # Use nanos instead of seconds.nanos because bash can't do math on decimals
  $ export TERASTASH_COUNTERS_DIR="$(mktemp --tmpdir -d ts-test-d-state.XXXXXXXXXX)"
  $ nanos-now > "$TERASTASH_COUNTERS_DIR/start"
  $ mkdir -p /tmp/mychunks
  $ ts list-chunk-stores
  $ ts define-chunk-store mychunks -t localfs -d /tmp/mychunks -s '80*1024'
  $ ts destroy unit_tests_d > /dev/null 2>&1 || true # In case the last test run was ctrl-c'ed

Can add and drop a file

  $ mkdir unit_tests_d
  $ cd unit_tests_d
  $ ts init unit_tests_d --chunk-store=mychunks
  $ echo hi > x
  $ touch --date=2015-01-01 x
  $ ts add x
  $ cd ..
  $ ts cat -n unit_tests_d x
  hi
  $ ts get -n unit_tests_d x
  $ cat x
  hi
  $ ts dump-db -n unit_tests_d
  {"~#Row":{"parent":"~bAAAAAAAAAAAAAAAAAAAAAA==","basename":"x","chunks_in_mychunks":null,"content":"~baGkK","crc32c":"~bG9ywgw==","executable":false,"key":null,"mtime":"~t2015-01-01T00:00:00.000Z","size":{"~#Long":"3"},"type":"f","uuid":null}}
  $ ts drop -n unit_tests_d x
  $ ts dump-db -n unit_tests_d
  $ ts mkdir -n unit_tests_d sub dir
  $ ts ls -n unit_tests_d -j ""
  dir
  sub
  $ ts mv -n unit_tests_d sub dir
  $ ts ls -n unit_tests_d -j ""
  dir

End

  $ echo "$(($(nanos-now) - $(cat "$TERASTASH_COUNTERS_DIR/start")))" | sed -r 's/(.........)$/\.\1/g' > "$TERASTASH_COUNTERS_DIR/duration"
