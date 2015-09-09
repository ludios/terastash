Setup

  $ function nanos-now() { date -u +%s%N; } # Use nanos instead of seconds.nanos because bash can't do math on decimals
  $ export TERASTASH_COUNTERS_DIR="$(mktemp --tmpdir -d ts-test-c-state.XXXXXXXXXX)"
  $ nanos-now > "$TERASTASH_COUNTERS_DIR/start"
  $ mkdir -p /tmp/mychunks-c
  $ ts define-chunk-store mychunks-c -t localfs -d /tmp/mychunks-c -s '80*1024'
  $ ts destroy unit_tests_c > /dev/null 2>&1 || true # In case the last test run was ctrl-c'ed
  $ ts init unit_tests_c --chunk-store=mychunks-c "--chunk-threshold=10*10"

Can run build-natives

  $ ts build-natives

Can create directories

  $ ts mkdir dir
  $ ls -1d dir # 'ts mkdir' should also create dir in working dir
  dir
  $ mkdir dir_already_exists_in_working_dir
  $ ts mkdir dir_already_exists_in_working_dir # 'ts mkdir' should work if dir already in working dir
  $ ts ls -j
  dir
  dir_already_exists_in_working_dir
  $ touch a
  $ ts add a
  $ rm a
  $ ts mkdir a # despite failing, this leaves behind an 'a' dir in working dir
  Cannot mkdir in database: 'a' in stash 'unit_tests_c' already exists as a file
  [1]
  $ touch a/b
  $ ts add a/b
  Cannot mkdir in database: 'a' in stash 'unit_tests_c' already exists as a file
  [1]
  $ touch c
  $ ts mkdir c
  Cannot mkdir in working directory: 'c' already exists and is not a directory
  [1]

Can move files to directories

  $ touch f
  $ ts add f
  $ ts mv f dir
  $ ls -1 | grep '^f$'
  [1]
  $ ts ls -j | grep '^f$' # file is no longer in src
  [1]
  $ ts ls -j dir # file is now in dest
  f
  $ ls -1 dir # file is moved in working directory as well
  f
  $ touch f
  $ ts add f
  $ ts mv f dir
  Cannot mv in database: destination parent=[0-9a-f]{32} basename='f' already exists in stash 'unit_tests_c' (re)
  [1]
  $ ts drop dir/f
  $ touch dir/b
  $ touch b
  $ ts add b
  $ ts mv b dir
  Cannot mv in working directory: refusing to overwrite .* (re)
  [1]
  $ mkdir sub1 sub2
  $ touch sub1/x sub2/y
  $ ts add sub1/x sub2/y
  $ ts mv sub1 sub2 dir/ # moving a directory into a directory works
  $ ts ls -j dir
  sub1
  sub2
  $ ts ls -j dir/sub1
  x
  $ ts mv dir/sub1/x ./
  $ ts ls -j dir/sub1
  $ ts ls -j | grep '^x$'
  x
  $ cd dir/sub2
  $ ts mv y ..
  $ ts ls -j .. | grep '^y$'
  y
  $ cd ../../

Can shoo files

  $ echo -n hi > hello
  $ touch --date=1980-01-01 hello
  $ ts add hello
  $ ln hello hello-hard-linked
  $ md5sum hello-hard-linked
  49f68a5c8493ec2c0bf489821c21fc3b  hello-hard-linked
  $ ts shoo hello
  $ md5sum hello-hard-linked # make sure hello-hard-linked was unaffected
  49f68a5c8493ec2c0bf489821c21fc3b  hello-hard-linked
  $ stat -c '%y' hello
  1980-01-01 00:00:00.000000000 +0000
  $ md5sum hello # md5sum is of two NULL bytes
  c4103f122d27677c9db144cae1394a66  hello
  $ mv hello hello2
  $ ts add hello2
  Refusing to add file 'hello2' because it has sticky bit set, which may have been set by 'ts shoo'
  [1]

Can get a fake file

  $ ts get --fake hello
  $ md5sum hello # md5sum is of two NULL bytes
  c4103f122d27677c9db144cae1394a66  hello

Cannot create files with illegal filenames

  $ touch 'con.c'
  $ ts add con.c
  Some Windows APIs do not support filenames whose non-extension component is 'CON'; got 'con.c'
  [1]
  $ ts mkdir aux.c
  Some Windows APIs do not support filenames whose non-extension component is 'AUX'; got 'aux.c'
  [1]
  $ ts mv hello aux.c
  Some Windows APIs do not support filenames whose non-extension component is 'AUX'; got 'aux.c'
  [1]

End

  $ echo "$(($(nanos-now) - $(cat "$TERASTASH_COUNTERS_DIR/start")))" | sed -r 's/(.........)$/\.\1/g' > "$TERASTASH_COUNTERS_DIR/duration"
