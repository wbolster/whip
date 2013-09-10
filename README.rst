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

* *Python* 2.7+
* *Plyvel* to access *LevelDB* from Python
* *Flask* for the REST API
* *Aaargh* for the command line tool
* *UltraJSON (*ujson*) for fast JSON encoding and decoding
