Setup

  $ function nanos-now() { date -u +%s%N; } # Use nanos instead of seconds.nanos because bash can't do math on decimals
  $ export TERASTASH_COUNTERS_DIR="$(mktemp --tmpdir -d ts-test-d-state.XXXXXXXXXX)"
  $ nanos-now > "$TERASTASH_COUNTERS_DIR/start"
  $ mkdir -p /tmp/mychunks
  $ ts list-chunk-stores
  $ ts define-chunk-store mychunks -t localfs -d /tmp/mychunks -s '100*1024'
  $ ts destroy unit_tests_d > /dev/null 2>&1 || true # In case the last test run was ctrl-c'ed

Can add and drop a file

  $ mkdir unit_tests_d
  $ cd unit_tests_d
  $ ts init unit_tests_d --chunk-store=mychunks
  $ echo hi > x
  $ ts add x
  $ cd ..
  $ ts cat -n unit_tests_d x
  hi
  $ ts get -n unit_tests_d x
  $ cat x
  hi

End

  $ echo "$(($(nanos-now) - $(cat "$TERASTASH_COUNTERS_DIR/start")))" | sed -r 's/(.........)$/\.\1/g' > "$TERASTASH_COUNTERS_DIR/duration"
