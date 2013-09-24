====
Whip
====

*Whip*, the who, what, where and when about IP address data.

Whip provides a fast IP information lookup service that also supports historical
lookups. Whip can build an index in LevelDB from source data files in JSON
format (one record per line), and allows for efficient lookups, also time-based.


Installation
============

Use a virtualenv to install Whip and its dependencies. To install from a source
tree::

    $ pip install -r requirements.txt
    $ pip install -e .


Usage
=====

Most functionality is provided by the `whip-cli` command line tool. Usage
information::

    $ whip-cli --help


Dependencies
============

* *Python* 3.3+ (no Python 2 support!)
* *Plyvel* to access *LevelDB* from Python
* *Flask* for the REST API
* *Aaargh* for the command line tool
* *UltraJSON (*ujson*) for fast JSON encoding and decoding


Ideas / TODO
============

* Perform range scan on in-memory structure instead seeking on a DB iterator.
  This means Whip must load all keys in memory on startup (in an `array.array`);
  use `bisect.bisect_right` to find the right entry, then simply `db.get()` for
  the actual value. To avoid scanning the complete database on startup, the
  keyspace should be split using a key prefix: one part keeps both the keys and
  values (full database), the other part only keeps the keys. The latter will be
  scanned and loaded into memory upon startup. For ~25000000 IPv4 addresses,
  keeping the index in memory only requires 100MB of RAM, and lookups would only
  issue `db.get()` for keys that are known to exist.

* Try out LMDB instead of LevelDB

* Support for adding new data to an existing database. Currently Whip only
  supports complete bulk imports.
