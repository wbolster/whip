"""
Whip database storage module.

All IP ranges with associated information are stored in a LevelDB
database. The key/value layout is as follows:

* The end IP is used as the key. This allows for quick
  fast range lookups.

* The begin IP and the actual information is stored in the value. The
  most recent information (as a dict) for a range is stored in full,
  encoded as JSON, so that it can be returned quickly without any
  decoding and processing overhead.

  To save a lot of space (and hence improve performance), historical
  data for an IP range is stored as diffs from the most recent version.
  When querying for older versions, the original data is reconstructed
  on-demand.

  The value is a 4-tuple packed using Msgpack like this:

  * IP begin address
  * JSON encoded data for the latest version
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

import collections
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
    ip_packed_to_int,
    ip_int_to_packed,
    ip_int_to_str,
    ip_str_to_packed,
    merge_ranges,
    PeriodicCallback,
    unique_justseen,
)


logger = logging.getLogger(__name__)
msgpack_dumps = msgpack.Packer().pack  # faster than calling .packb()
msgpack_dumps_utf8 = msgpack.Packer(encoding='UTF-8').pack  # idem

DATETIME_GETTER = operator.itemgetter('datetime')


def debug_format_dict(d):  # pragma: no cover
    """Formatting function for debugging purposes"""
    return ', '.join('%s=%s' % (k[:1], v or '') for k, v in sorted(d.items()))


def make_squash_key(d):
    """Dict squashing key function"""

    # Compare all data except for the 'datetime' key
    d = d.copy()
    d.pop('datetime')
    return d


def build_record(begin_ip_int, end_ip_int, dicts):
    """Create database records for an iterable of merged dicts."""

    assert len(dicts) > 0

    # Deduplicate. Each dict will have a different timestamp, so sort
    # chronologically, and ignore the datetime while deduplicating.
    dicts.sort(key=DATETIME_GETTER)
    unique_dicts = list(unique_justseen(dicts, key=make_squash_key))

    # The most recent data is stored in full, while older dicts are
    # stored in a history structure with (reverse) diffs for each pair.
    # This saves a lot of storage space, but requires "patching" during
    # lookups. The benefits of storing smaller values (less storage
    # space, hence faster lookups) outweigh this disadvantage though.
    unique_dicts.reverse()
    latest, patches = dict_diff_incremental(unique_dicts)

    # Build the actual key and value byte strings.
    key = ip_int_to_packed(end_ip_int)
    value = msgpack_dumps((
        ip_int_to_packed(begin_ip_int),
        json_dumps(latest, ensure_ascii=False).encode('UTF-8'),
        latest['datetime'].encode('ascii'),
        msgpack_dumps_utf8(list(patches)),
    ))
    return key, value


def msgpack_loads_patches(packed):
    """Load a Msgpack encoded list of patches from a byte string"""
    return msgpack_loads(packed, use_list=False, encoding='UTF-8')


ExistingRecord = collections.namedtuple(
    'ExistingRecord',
    ('latest_json', 'history_msgpack'))


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

    def iter_records(self):
        """
        Iterate a database and yield records that can be merged with new data.

        This generator is suitable for consumption by merge_ranges().
        """
        for key, value in self.db.iterator(fill_cache=False):
            unpacked = msgpack_loads(value, use_list=False)
            begin_ip_packed, latest_json, _, history_msgpack = unpacked
            begin_ip = ip_packed_to_int(begin_ip_packed)
            end_ip = ip_packed_to_int(key)
            record = ExistingRecord(latest_json, history_msgpack)
            yield begin_ip, end_ip, record

    def load(self, *iterables):
        """Load data from importer iterables"""

        if not iterables:
            logger.warning("No new input files; nothing to load")
            return

        # Combine new data with current database contents, and merge all
        # iterables to produce unique, non-overlapping ranges.
        iterables = list(iterables)
        iterables.append(self.iter_records())
        merged = merge_ranges(*iterables)

        # Progress/status tracking
        n_processed = n_updated = 0
        begin_ip_int = 0
        reporter = PeriodicCallback(lambda: logger.info(
            "%d ranges processed (%d updated, %d new); current position %s",
            n_processed, n_updated, n_processed - n_updated,
            ip_int_to_str(begin_ip_int)))
        reporter.tick()

        # Loop over current database and new data
        for begin_ip_int, end_ip_int, items in merged:
            if n_processed % 100 == 0:
                reporter.tick()

            # Find and pop existing record (if any) from the list.
            existing = None
            for idx, item in enumerate(items):
                if isinstance(item, ExistingRecord):
                    existing = item
                    del items[idx]
                    break

            # If the database already contained a record spanning this
            # range, combine it with the newly added data.
            if existing is not None:
                latest = json_loads(existing.latest_json)
                items.append(latest)
                patches = msgpack_loads_patches(existing.history_msgpack)
                items.extend(
                    dict_patch_incremental(latest, patches, inplace=False))
                n_updated += 1

            # Build and store a new record
            key, value = build_record(begin_ip_int, end_ip_int, items)
            self.db.put(key, value)
            n_processed += 1

        reporter.tick(True)

        logger.info("Compacting database...")
        self.db.compact_range(
            start=b'\x00\x00\x00\x00',
            stop=b'\xff\xff\xff\xff')

        # Force lookups to use a new iterator so new data is seen.
        self.iter = None

    @functools.lru_cache(128 * 1024)
    def lookup(self, ip, datetime=None):
        """Lookup a single IP address in the database.

        This function returns the found information as a JSON byte
        string (encoded as UTF-8), or `None` if no information was
        found.

        If `datetime` is `None`, the latest version is returned. If
        `datetime` is a datetime string, information for that timestamp
        is returned. If `datetime` has the special value 'all', the full
        history will be returned.
        """

        ip_packed = ip_str_to_packed(ip)

        # Iterator construction is relatively costly, so reuse it for
        # performance reasons. The iterator won't see any data written
        # after its construction, but that is not a problem since the
        # data set is static.
        if self.iter is None:
            self.iter = self.db.iterator(include_key=False)

        # The database key stores the end IP of all ranges, so a simple
        # seek positions the iterator at the right key (if found).
        self.iter.seek(ip_packed)
        value = next(self.iter, None)

        # If the seek moved past the last range in the database: no hit
        if value is None:
            return None

        # Decode the value
        value = msgpack_loads(value, use_list=False)
        begin_ip_packed, latest_json, latest_datetime, history_msgpack = value

        # Check range boundaries. If the IP currently being looked up is
        # in a gap, there is no hit after all.
        if ip_packed < begin_ip_packed:
            return None

        # If the lookup is for the most recent version, we're done. No
        # decoding required.
        if datetime is None:
            return latest_json

        return_history = (datetime == 'all')

        # The most recent version may be the one asked for. No decoding
        # required in that case.
        if not return_history and latest_datetime.decode('ascii') <= datetime:
            return latest_json

        # The history is actually needed to answer the query. Decode
        # both the latest version and the history.
        d = json_loads(latest_json)
        patches = msgpack_loads_patches(history_msgpack)

        # Reconstruct complete history if the query asks for all
        # historical data. The JSON response is an object (not a list)
        # for security reasons.
        if return_history:
            history = [d]
            history.extend(dict_patch_incremental(d, patches, inplace=False))
            out = {'history': history}
            return json_dumps(out, ensure_ascii=False).encode('UTF-8')

        # This is a lookup for a specific timestamp. Iteratively
        # apply patches until (hopefully) a match is found.
        for d in dict_patch_incremental(d, patches, inplace=True):
            if d['datetime'] <= datetime:
                return json_dumps(d, ensure_ascii=False).encode('UTF-8')

        # Too bad, no result
        return None
