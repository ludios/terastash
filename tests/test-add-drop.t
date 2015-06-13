Can add and drop a file

  $ mkdir -p /tmp/mychunks
  $ ts list-chunk-stores
  $ ts define-chunk-store mychunks -t localfs -d /tmp/mychunks -s '100*1024'
  $ ts destroy unit_tests_a > /dev/null 2>&1 || true # In case the last test run was ctrl-c'ed
  $ ts init unit_tests_a --chunk-store=mychunks
  $ ts list-chunk-stores
  mychunks
  $ echo -e "hello\nworld" > sample1
  $ touch --date=1970-01-01 sample1
  $ echo -e "second\nsample" > sample2
  $ touch --date=1980-01-01 sample2
  $ chmod +x sample2
  $ dd bs=1024 count=1024 if=/dev/zero of=bigfile 2> /dev/null
  $ cat bigfile | md5sum | cut -f 1 -d " "
  b6d81b360a5672d80c27430f39153e2c
  $ touch --date=1990-01-01 bigfile
  $ ts add sample1 sample2 bigfile
  $ ts ls -n unit_tests_a
  When using -n/--name, a database path is required
  [1]
  $ ts ls
           1,048,576 1990-01-01 00:00 bigfile
                  12 1970-01-01 00:00 sample1
                  14 1980-01-01 00:00 sample2*
  $ ts ls -t
           1,048,576 1990-01-01 00:00 bigfile
                  14 1980-01-01 00:00 sample2*
                  12 1970-01-01 00:00 sample1
  $ ts ls -rt
                  12 1970-01-01 00:00 sample1
                  14 1980-01-01 00:00 sample2*
           1,048,576 1990-01-01 00:00 bigfile
  $ ts ls -j
  bigfile
  sample1
  sample2
  $ ts ls -rj
  sample2
  sample1
  bigfile
  $ ts ls -j -n unit_tests_a ''
  bigfile
  sample1
  sample2
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
  $ ts get sample1
  $ stat -c %y sample1
  1970-01-01 00:00:00.000000000 +0000
  $ cat sample1
  hello
  world
  $ rm sample1 bigfile
  $ ts cat bigfile > bigfile.copy
  $ cat bigfile.copy | md5sum | cut -f 1 -d " "
  b6d81b360a5672d80c27430f39153e2c
  $ ts get sample1 bigfile # Make sure 'ts get' works with > 1 file
  $ stat -c %y sample1
  1970-01-01 00:00:00.000000000 +0000
  $ stat -c %y bigfile
  1990-01-01 00:00:00.000000000 +0000
  $ cat bigfile | md5sum | cut -f 1 -d " "
  b6d81b360a5672d80c27430f39153e2c
  $ ts drop sample1 bigfile
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
  $ ts drop d1

Dropping file again is a no-op

  $ ts drop sample1
  $ ts ls

Dropping nonexistent file is a no-op

  $ ts drop doesntexist
  $ ts ls

Can list stashes

  $ ts list-stashes | grep -P '^unit_tests_a$'
  unit_tests_a

Can destroy a terastash

  $ ts destroy unit_tests_a
  Destroyed keyspace and removed config for unit_tests_a.

Stash is not listed after being destroyed

  $ ts list-stashes | grep -P '^unit_tests_a$'
  [1]

Can store chunks in gdrive

  $ ts destroy unit_tests_b > /dev/null 2>&1 || true # In case the last test run was ctrl-c'ed
  $ ts init unit_tests_b --chunk-store=terastash-tests-gdrive
  $ ts config-chunk-store terastash-tests-gdrive --chunk-size=1024
  $ dd bs=1025 count=2 if=/dev/urandom of=smallfile 2> /dev/null
  $ MD5_BEFORE="$(cat smallfile | md5sum | cut -f 1 -d " ")"
  $ ts add smallfile
  $ rm smallfile
  $ ts get smallfile
  $ MD5_AFTER="$(cat smallfile | md5sum | cut -f 1 -d " ")"
  $ [[ "$MD5_BEFORE" == "$MD5_AFTER" ]]
