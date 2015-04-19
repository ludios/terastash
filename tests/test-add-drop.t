Can add and drop a file

  $ ts init unit_tests
  Created Cassandra keyspace and updated terastash.json.
  $ echo -e "hello\nworld" > sample1
  $ touch --date=1970-01-01 sample1
  $ echo -e "second\nsample" > sample2
  $ touch --date=1980-01-01 sample2
  $ ts add sample1 sample2
  $ ts ls
                  14 1980-01-01 00:00 sample2
                  12 1970-01-01 00:00 sample1
  $ ts ls -j
  sample2
  sample1
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
  $ ts drop sample1
  $ ts ls
                  14 1980-01-01 00:00 sample2
  $ ts drop sample2
  $ ts ls

Parent directories are automatically created as needed

  $ mkdir dir
  $ touch dir/empty
  $ ts add dir/empty
  $ ts ls -j dir
  empty
  $ ts ls -j
  dir
  $ ts drop dir

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
