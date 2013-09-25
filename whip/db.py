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
  * Length of the latest datetime string (1 byte)
  * Latest datetime as string
  * JSON encoded diffs for older versions (variable length, until end)

"""

import logging
import operator
import struct

import plyvel

from whip.json import dumps, loads
from whip.util import (
    dict_diff_decremental,
    dict_patch,
    ipv4_int_to_bytes,
    ipv4_int_to_str,
    merge_ranges,
    PeriodicCallback,
    squash_duplicate_dicts,
)


logger = logging.getLogger(__name__)

DATETIME_GETTER = operator.itemgetter('datetime')
uint8_pack = struct.Struct('>B').pack
uint16_pack = struct.Struct('>H').pack
uint16_unpack = struct.Struct('>H').unpack


def _debug_format_infoset(d):
    """Formatting function for debugging purposes"""
    return ', '.join('%s=%s' % (k[:1], v or '')
                     for k, v in sorted(d.items()))


def build_record(begin_ip_int, end_ip_int, infosets):
    """Create database records for an iterable of merged infosets."""

    assert len(infosets) > 0

    # Deduplicate. Each infoset will have a different timestamp, so sort
    # chronologically, and ignore the datetime while deduplicating.
    infosets.sort(key=DATETIME_GETTER)
    unique_infosets = squash_duplicate_dicts(infosets, ignored_key='datetime')

    # The most recent infoset is stored in full
    latest = unique_infosets[-1]
    latest_datetime = latest['datetime'].encode('ascii')
    latest_json = dumps(latest, ensure_ascii=False).encode('UTF-8')

    # Older infosets are stored in a history structure with (reverse)
    # diffs for each pair. This saves a lot of storage space, but
    # requires "patching" during lookups. Since the storage layer is
    # faster when working with smaller values, this is a trade-off
    # between storage size and retrieval speed: either store less data
    # (faster) and decode it when querying (slower), or store more data
    # (slower) without any decoding (faster). The former works better in
    # practice, especially when data sizes grow.
    history = dict_diff_decremental(unique_infosets)
    history_json = dumps(history, ensure_ascii=False).encode('UTF-8')

    # Build the actual key and value byte strings.
    # XXX: String concatenation seems faster than the''.join((..., ...))
    # alternative on 64-bit CPython 2.7.5.
    key = ipv4_int_to_bytes(end_ip_int)
    value = (ipv4_int_to_bytes(begin_ip_int)
             + uint16_pack(len(latest_json))
             + latest_json
             + uint8_pack(len(latest_datetime))
             + latest_datetime
             + history_json)
    return key, value


class Database(object):
    """
    Database access class for loading and looking up data.
    """

    def __init__(self, database_dir, create_if_missing=False):
        logger.debug("Opening database %s", database_dir)
        self.db = plyvel.DB(
            database_dir,
            create_if_missing=create_if_missing,
            write_buffer_size=16 * 1024 * 1024,
            max_open_files=512,
            lru_cache_size=128 * 1024 * 1024)
        self.iter = None
        self._refresh_iter()

    def _refresh_iter(self):
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
        item = None  # this makes pylint happy
        for n, item in enumerate(merged, 1):
            key, value = build_record(*item)
            self.db.put(key, value)

            # Tick once in a while
            if n % 100 == 0:
                reporter.tick()

        if n > 0:
            reporter.tick(True)

        # Refresh iterator so that it sees the new data
        self._refresh_iter()

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
        offset = 4
        (json_size,) = uint16_unpack(value[offset:offset + 2])
        offset += 2
        infoset_json = value[offset:offset + json_size]

        # If the lookup is for the most recent version, we're done.
        if dt is None:
            return infoset_json

        # This is a lookup for a specific timestamp. The most recent
        # version may be the one asked for.
        offset += json_size
        latest_datetime_size = value[offset]  # gives an int
        offset += 1
        latest_datetime = value[offset:offset + latest_datetime_size]
        if latest_datetime <= dt.encode('ascii'):
            return infoset_json

        offset += latest_datetime_size

        # Too bad, we need to delve deeper into history. Decode JSON,
        # iteratively apply patches, and re-encode to JSON again.
        infoset = loads(infoset_json)
        history = loads(value[offset:])
        for to_delete, to_set in history:
            dict_patch(infoset, to_delete, to_set)
            if infoset['datetime'] <= dt:
                # Finally found it; encode and return the result.
                return dumps(infoset)

        # Too bad, no result
        return None
