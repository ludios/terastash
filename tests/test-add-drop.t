Can add and drop a file

  $ mkdir -p /tmp/mychunks
  $ ts list-chunk-stores
  $ ts define-chunk-store mychunks -t localfs -d /tmp/mychunks
  $ ts destroy unit_tests > /dev/null 2>&1 || true # In case the last test run was ctrl-c'ed
  $ ts init unit_tests --chunk-store=mychunks
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
  $ ts ls -n unit_tests
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
  $ ts ls -j -n unit_tests ''
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
  $ ls -1 sample1
  sample1
  $ cat sample1
  hello
  world
  $ rm bigfile
  $ ts get bigfile
  $ cat bigfile | md5sum | cut -f 1 -d " "
  b6d81b360a5672d80c27430f39153e2c
  $ ts drop sample1 bigfile
  $ ts ls
                  14 1980-01-01 00:00 sample2*
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

  $ ts list-stashes | grep -P '^unit_tests$'
  unit_tests

Can destroy a terastash

  $ ts destroy unit_tests
  Destroyed keyspace ts_unit_tests.

Stash is not listed after being destroyed

  $ ts list-stashes | grep -P '^unit_tests$'
  [1]
