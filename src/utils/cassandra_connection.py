import sys

if sys.platform == "win32":
    from cassandra.io.geventreactor import GeventConnection as CassandraConnection
else:
    from cassandra.io.libevreactor import LibevConnection as CassandraConnection