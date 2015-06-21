Setup

  $ export TERASTASH_COUNTERS_DIR="$(mktemp --tmpdir -d ts-test-b-state.XXXXXXXXXX)"
  $ mkdir -p "$HOME/.config/terastash"
  $ cp -a "$REAL_HOME/.config/terastash/chunk-stores.json" "$HOME/.config/terastash/"
  $ cp -a "$REAL_HOME/.config/terastash/google-tokens.json" "$HOME/.config/terastash/"
  $ ts destroy unit_tests_b > /dev/null 2>&1 || true # In case the last test run was ctrl-c'ed
  $ ts init unit_tests_b --chunk-store=terastash-tests-gdrive "--chunk-threshold=10*10"

Can store chunks in gdrive

  $ ts config-chunk-store terastash-tests-gdrive --chunk-size=1024
  $ dd bs=1025 count=2 if=/dev/urandom of=smallfile 2> /dev/null
  $ MD5_BEFORE="$(cat smallfile | md5sum | cut -f 1 -d " ")"
  $ ts add smallfile
  $ rm smallfile
  $ ts get smallfile
  $ MD5_AFTER="$(cat smallfile | md5sum | cut -f 1 -d " ")"
  $ [[ "$MD5_BEFORE" == "$MD5_AFTER" ]]
  $ ts drop smallfile
