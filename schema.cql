CREATE KEYSPACE IF NOT EXISTS terastash WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE terastash;

CREATE TABLE IF NOT EXISTS fs (
	pathname text PRIMARY KEY,
	parent text,
	content blob,
	sha256sum blob
);
