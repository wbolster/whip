====
Whip
====

Overview
========

*Whip*, the who, what, where and when about IP address data.

Whip provides a fast IP information lookup service that scales to large data
sets. Whip can efficiently answer queries for a specific IP address, optionally
refined by a timestamp.

Feature summary:

* Fast lookups for IP addresses, optionally limited to a specific timestamp.
  This means that data that (slowly) changes over time can be queried at any
  known point in history.

* Transparent support for both IPv4 and IPv6 addresses and ranges

* Support for incremental loading. This means new data can be added to an
  existing database. This is useful to ingest weekly data set snapshots.

* Efficient storage format (data stored as ranges, history as diffs)

* Transparent support for gzip compressed input data the file name ends with
  ``.gz``

Whip can handle almost any data set containing information about IP address
ranges. Whip does not limit the type of data that is associated to an IP address
or IP address range, so it can be used for a variety of data sets, for instance
IP geolocation data sets. Note that Whip itself does *not* come with any data
sets.

A source file should be seen as a 'snapshot' of an evolving data set at a
specific point in time. During the loading phase, Whip combines all snapshots
(with different timestamps) and constructs the history of all records in a way
that can be queried efficiently. It does so by building an index of IP ranges
and associating data to each range (e.g. geolocation tags), and keeping history
for each distinct range.


Installation
============

Use a virtualenv to install Whip and its dependencies. To install from a source
tree::

    $ pip install -r requirements.txt


These are the current dependencies:

* *Python* 3.3+ (no Python 2 support!)
* *Plyvel* to access *LevelDB* from Python
* *Flask* for the REST API
* *Aaargh* for the command line tool
* *UltraJSON (ujson)* for fast JSON encoding and decoding
* *Msgpack* for Msgpack encoding and decoding


Usage
=====

Command line interface
----------------------

Most functionality is provided by the `whip-cli` command line tool. Detailed
usage information::

    $ whip-cli --help

To load input data into ``my.db`` (creating it when necessary)::

    $ whip-cli --db my.db load input-file-1.json.gz input-file-2.json.gz

To serve the database over a REST API::

    $ whip-cli --db my.db serve

The REST API can also be deployed using WSGI e.g. using gunicorn/nginx.

REST API
--------

The REST API supports these queries:

* To retrieve the latest record for an IP address::

      GET /ip/1.2.3.4

* To obtain a specific record for an IP address::

      GET /ip/1.2.3.4?datetime=2013-05-15

* To obtain the complete history for an IP address::

      GET /ip/1.2.3.4?datetime=all

In each case, the response will be an ``application/json`` encoded document,
even if no hit was found, in which case the result will be an empty JSON
document. HTTP status codes are only used to signify errors.

Input data format
-----------------

The source file format is simple: it's just a a text file with one
JSON-formatted document on each line. Each document can contain arbitrary
information about a range of IP addresses.

Whip itself requires three fields:

* ``begin``: begin of IP address range (inclusive), e.g. ``1.0.0.0``
* ``end``: end of IP address range (inclusive), e.g. ``1.0.255.255``
* ``datetime``: time stamp for this record, e.g. ``2013-02-28``

In addition to these fields, each document may contain arbitrary key/value
pairs, e.g. ``"country"``.

An input data file must follow these rules:

* Each range in the file must span at least 1 IP address, but can span an
  arbitrary number of consecutive IP addresses (not limited by net block/CIDR),
  specified in the ``begin`` and ``end`` fields.

* Ranges in a single input file must not overlap, but there may be gaps between
  ranges (in case no information is available for that range).

* Ranges in a single input file must be sorted by IP address. IPv4 ranges must
  sort *before* IPv6 ranges, since Whip uses RFC3493 IPv4-mapped IPv6 addresses
  internally.

* Timestamps, specified in ``datetime`` field, should be in ISO8601 format,
  since Whip depends on the lexicographic ordering of the input strings.

* All timestamps in a single input file must be the same. Yes, this adds
  redundancy, but avoids the need for header lines or out-of-band metadata.

.. warning::

   If the input data does not follow the above rules, bad things may happen,
   including database corruption.

An example input document looks like this (formatted on multiple lines for
clarity)::

    {
        "begin": "1.0.0.0",
        "end": "1.255.255.255",
        "datetime": "2013-02-28",
        "location": "Amsterdam",
        "some-other-data": "anything-you-like"
    }

A single input file with many of these documents looks like this::

    {"begin": "1.0.0.0", "end": "1.255.255.255", "datetime": "2013-02-28", "...": "..."}
    {"begin": "2.0.0.0", "end": "2.255.255.255", "datetime": "2013-02-28", "...": "..."}
    {"begin": "11.0.0.0", "end": "11.0.255.255", "datetime": "2013-02-28", "...": "..."}

Whip can load many of these input files (e.g. weekly snapshots for a longer
period of time) in a single loading pass.


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

  Update: experiments on a big database containing most IP ranges in use show
  this is *not* any faster than doing the actual seek, since `it.seek()` takes
  just as long as `db.get()`. This means a lot of memory will be used to improve
  performance for non-hits (in which case no DB calls are made).

* Try out LMDB instead of LevelDB

* Pluggable storage backends (e.g. HBase)
