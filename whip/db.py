"""
Whip database storage module.

All IP ranges with associated information are stored in a LevelDB
database. The key/value layout is as follows:

* The end IP is used as the key. This allows for quick
  fast range lookups.

* The begin IP and the actual information is stored in the value. The
  most recent infoset for a range is stored in full, encoded as JSON, so
  that it can be returned quickly without any decoding and processing
  overhead.

  To save a lot of space (and hence improve performance), historical
  data for an IP range is stored as diffs from the most recent version.
  When querying for older versions, the original data is reconstructed
  on-demand.

  The value is packed as follows:

  * IPv4 begin address (4 bytes)
  * Length of the JSON data for the most recent information (2 bytes)
  * JSON encoded data for the latest version (variable length)
  * JSON encoded diffs for older versions (variable length)

"""

import logging
import operator
import struct

import plyvel
import ujson

from whip.util import (
    dict_diff,
    dict_patch,
    ipv4_int_to_bytes,
    ipv4_int_to_str,
    merge_ranges,
    PeriodicCallback,
)

SIZE_STRUCT = struct.Struct('>H')

logger = logging.getLogger(__name__)


def _build_db_record(begin_ip_int, end_ip_int, infosets):
    """Create database records for an iterable of merged infosets."""

    # Build history structure. The latest version is stored in
    # full, ...
    infosets.sort(key=operator.itemgetter('datetime'), reverse=True)
    latest = infosets[0]
    latest_json = ujson.dumps(latest)

    # ... while older versions are stored as (reverse) diffs to the
    # previous (in time) version.
    history_json = ujson.dumps([
        dict_diff(infosets[i + 1], infosets[i])
        for i in xrange(len(infosets) - 1)
    ])

    # Build the actual key and value byte strings
    key = ipv4_int_to_bytes(end_ip_int)
    value = (ipv4_int_to_bytes(begin_ip_int)
             + SIZE_STRUCT.pack(len(latest_json))
             + latest_json
             + history_json)
    return key, value


class Database(object):
    def __init__(self, database_dir, create_if_missing=False):
        logger.debug("Opening database %s", database_dir)
        self.db = plyvel.DB(
            database_dir,
            create_if_missing=create_if_missing,
            write_buffer_size=16 * 1024 * 1024,
            max_open_files=512,
            lru_cache_size=128 * 1024 * 1024)
        self._make_iter()

    def _make_iter(self):
        """Make an iterator for the current database.

        Iterator construction is relatively costly, so reuse it for
        performance reasons. The iterator won't see any data written
        after its construction, but this is not a problem since the data
        set is static.
        """
        self.iter = self.db.iterator(include_key=False)

    def load(self, *iters):
        """Load data from importer iterables"""

        # Merge all iterables to produce unique, non-overlapping IP
        # ranges with multiple timestamped infosets.
        merged = merge_ranges(*iters)

        reporter = PeriodicCallback(lambda: logger.info(
            "%d database records stored; current position: %s",
            n, ipv4_int_to_str(item[0])))

        n = 0
        for n, item in enumerate(merged, 1):
            key, value = _build_db_record(*item)
            self.db.put(key, value)
            reporter.tick()

        reporter.tick(True)

        # Refresh iterator so that it sees the new data
        self._make_iter()

    def lookup(self, ip, dt=None):
        """Lookup a single IP address in the database

        This either returns the stored information, or `None` if no
        information was found.
        """

        # The database key stores the end IP of all ranges, so a simple
        # seek positions the iterator at the right key (if found).
        self.iter.seek(ip)
        try:
            value = next(self.iter)
        except StopIteration:
            # Past any range in the database: no hit
            return None

        # Check range boundaries. The first 4 bytes store the begin IP.
        # If the IP currently being looked up is in a gap, there is no
        # hit after all.
        if ip < value[:4]:
            return None

        # The next 2 bytes indicate the length of the JSON string for
        # the most recent information
        (size,) = SIZE_STRUCT.unpack(value[4:6])
        infoset_json = value[6:size + 6]

        # If the lookup is for the most recent version, we're done
        if dt is None:
            return infoset_json

        # This is a lookup for a specific timestamp. This means we
        # actually need to peek into the record.
        infoset = ujson.loads(infoset_json)

        # The most recent version may be the one asked for.
        if infoset['datetime'] <= dt:
            # TODO: store latest date somewhere more easily accessible
            # (timestamp field after the JSON length field perhaps?) to
            # avoid JSON parsing overhead for this case.
            return infoset_json

        # Too bad, we need to delve deeper into history by iteratively
        # applying patches.
        history = ujson.loads(value[size + 6:])
        for to_delete, to_set in history:
            dict_patch(infoset, to_delete, to_set)
            if infoset['datetime'] <= dt:
                # Finally found it; encode and return the result
                return ujson.dumps(infoset)

        # Too bad, no result
        return None
