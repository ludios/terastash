Can add and drop a file

  $ ts init unit_tests
  Created Cassandra keyspace and updated terastash.json.
  $ echo -e "hello\nworld" > sample
  $ ts add sample
  $ ts ls
  sample
  $ ts cat sample
  hello
  world
  $ rm -f sample
  $ ls -1 sample
  ls: cannot access sample: No such file or directory
  [2]
  $ ts get sample
  $ ls -1 sample
  sample
  $ cat sample
  hello
  world
  $ ts drop sample
  $ ts ls

Dropping file again is a no-op

  $ ts drop sample
  $ ts ls

Dropping nonexistent file is a no-op

  $ ts drop doesntexist
  $ ts ls

Can destroy a terastash

  $ ts destroy unit_tests
  Destroyed keyspace ts_unit_tests.
