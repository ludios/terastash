Can add and drop a file

  $ ts init ts_unit_tests
  Created Cassandra keyspace and updated terastash.json.
  $ touch empty
  $ ts add empty
  $ ts ls
  empty
  $ ts drop empty
  $ ts ls

Dropping file again is a no-op

  $ ts drop empty
  $ ts ls

Dropping nonexistent file is a no-op

  $ ts drop doesntexist
  $ ts ls
