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

  The value is a 4-tuple packed using Msgpack like this:

  * IPv4 begin address (4 bytes)
  * JSON encoded data for the latest version (variable length)
  * Latest datetime
  * Msgpack encoded diffs for older versions (yes, Msgpack in Msgpack,
    since this nested structure is not always needed and lets us decode
    it explicitly)

Note the odd mix of JSON and Msgpack encoding. Encoding/decoding speeds
are comparable (when using ujson), but Msgpack uses less space and hence
makes LevelDB faster, so that is the preferred format. The one exception
is the 'latest version' data, which is encoded using JSON, since that
saves a complete decode/encode (from Msgpack to JSON) roundtrip when
executing queries asking for the most recent version.
"""

import functools
import logging
import operator

import msgpack
from msgpack import loads as msgpack_loads
import plyvel

from .json import dumps as json_dumps, loads as json_loads
from .util import (
    dict_diff_incremental,
    dict_patch_incremental,
    ipv4_int_to_bytes,
    ipv4_int_to_str,
    merge_ranges,
    PeriodicCallback,
    squash_duplicate_dicts,
)


logger = logging.getLogger(__name__)
msgpack_dumps = msgpack.Packer().pack  # faster than calling .packb()
msgpack_dumps_utf8 = msgpack.Packer(encoding='UTF-8').pack  # idem

DATETIME_GETTER = operator.itemgetter('datetime')


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

    # The most recent infoset is stored in full, while older infosets are
    # stored in a history structure with (reverse) diffs for each pair. This
    # saves a lot of storage space, but requires "patching" during lookups.
    # Since the storage layer is faster when working with smaller values, this
    # is a trade-off between storage size and retrieval speed: either store
    # less data (faster) and decode it when querying (slower), or store more
    # data (slower) without any decoding (faster). The former works better in
    # practice, especially when data sizes grow.
    unique_infosets.reverse()
    latest, history = dict_diff_incremental(unique_infosets)
    history = list(history)  # consume iterator before serialization

    # Build the actual key and value byte strings.
    key = ipv4_int_to_bytes(end_ip_int)
    value = msgpack_dumps((
        ipv4_int_to_bytes(begin_ip_int),
        json_dumps(latest, ensure_ascii=False).encode('UTF-8'),
        latest['datetime'].encode('ascii'),
        msgpack_dumps_utf8(history),
    ))
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

    def load(self, *iters):
        """Load data from importer iterables"""

        # Merge all iterables to produce unique, non-overlapping IP
        # ranges with multiple timestamped infosets.
        merged = merge_ranges(*iters)

        reporter = PeriodicCallback(lambda: logger.info(
            "%d database records stored; current position: %s",
            n, ipv4_int_to_str(begin_ip_int)))  # pylint: disable=W0631

        n = 0
        for begin_ip_int, end_ip_int, infosets in merged:
            n += 1
            if n % 100 == 1:
                reporter.tick()

            key, value = build_record(begin_ip_int, end_ip_int, infosets)
            self.db.put(key, value)

        if n > 0:
            reporter.tick(True)

        # Force lookups to use a new iterator so new data is seen.
        self.iter = None

    @functools.lru_cache(128 * 1024)
    def lookup(self, ip, dt=None):
        """Lookup a single IP address in the database

        This either returns the stored information, or `None` if no
        information was found.
        """

        # Iterator construction is relatively costly, so reuse it for
        # performance reasons. The iterator won't see any data written
        # after its construction, but that is not a problem since the
        # data set is static.
        if self.iter is None:
            self.iter = self.db.iterator(include_key=False)

        # The database key stores the end IP of all ranges, so a simple
        # seek positions the iterator at the right key (if found).
        self.iter.seek(ip)
        try:
            value = next(self.iter)
        except StopIteration:
            # Past any range in the database: no hit
            return None

        # Decode the value
        value = msgpack_loads(value, use_list=False)
        begin_ip, latest_json, latest_datetime, history_msgpack = value

        # Check range boundaries. If the IP currently being looked up is
        # in a gap, there is no hit after all.
        if ip < begin_ip:
            return None

        # If the lookup is for the most recent version, we're done.
        if dt is None:
            return latest_json

        # This is a lookup for a specific timestamp. The most recent
        # version may be the one asked for.
        if latest_datetime <= dt.encode('ascii'):
            return latest_json

        # Too bad, we need to delve deeper into history. Decode JSON,
        # iteratively apply patches, and re-encode to JSON again.
        infoset = json_loads(latest_json)
        history = msgpack_loads(
            history_msgpack, use_list=False, encoding='UTF-8')
        for infoset in dict_patch_incremental(infoset, history, inplace=True):
            if infoset['datetime'] <= dt:
                # Finally found it; encode and return the result.
                return json_dumps(infoset, ensure_ascii=False).encode('UTF-8')

        # Too bad, no result
        return None
