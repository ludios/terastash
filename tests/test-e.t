Setup

  $ function nanos-now() { date -u +%s%N; } # Use nanos instead of seconds.nanos because bash can't do math on decimals
  $ export TERASTASH_COUNTERS_DIR="$(mktemp --tmpdir -d ts-test-e-state.XXXXXXXXXX)"
  $ nanos-now > "$TERASTASH_COUNTERS_DIR/start"
  $ mkdir -p /tmp/mychunks-e
  $ ts define-chunk-store mychunks-e -t localfs -d /tmp/mychunks-e -s '80*1024'
  $ ts destroy unit_tests_e > /dev/null 2>&1 || true # In case the last test run was ctrl-c'ed
  $ ts init unit_tests_e --chunk-store=mychunks-e "--chunk-threshold=10*10"

ts find from the root directory

  $ ts mkdir a a/sub
  $ touch a/x a/sub/g b c d
  $ ts add a/x a/sub/g b c d
  $ ts find
  a
  a/sub
  a/sub/g
  a/x
  b
  c
  d
  $ ts find -t f
  a/sub/g
  a/x
  b
  c
  d
  $ ts find -t d
  a
  a/sub
  $ ts find a/sub
  g
  $ cd a
  $ ts find
  sub
  sub/g
  x
  $ ts find ..
  a
  a/sub
  a/sub/g
  a/x
  b
  c
  d
  $ cd ..

ts find -0

  $ ts find -0 | python -c 'import sys; print repr(sys.stdin.read())'
  'a\x00a/sub\x00a/sub/g\x00a/x\x00b\x00c\x00d\x00'

End

  $ echo "$(($(nanos-now) - $(cat "$TERASTASH_COUNTERS_DIR/start")))" | sed -r 's/(.........)$/\.\1/g' > "$TERASTASH_COUNTERS_DIR/duration"
