"""
Whip database storage module.

All IP ranges with associated information are stored in a LevelDB
database.

Some remarks about the construction of database records:

* The most recent version of a record is stored in full, and older dicts
  are stored in a history structure of (reverse) diffs. This saves a lot
  of storage space and positively affects performance, since the
  benefits of storing/retrieving less data outweigh the cost of
  reconstructing historical records for historical lookups.

* Before creating diffs, the data will be deduplicated by
  'squashing' unchanged records and only storing new versions when they
  were first seen. Since each dict will have a different timestamp, the
  datetime will be ignored while deduplicating.

The key/value layout is as follows:

* The end IP is used as the key. This allows for fast lookups since it
  requires only a single seek and a single record.

* The begin IP and the actual information is stored in the value,
  packed using Msgpack like this:

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


def build_key_value(begin_ip_int, end_ip_int, latest_json, latest_datetime,
                    history_msgpack):
    """Build the actual key and value byte strings"""
    key = ip_int_to_packed(end_ip_int)
    value = msgpack_dumps((
        ip_int_to_packed(begin_ip_int),
        latest_json,
        latest_datetime.encode('ascii'),
        history_msgpack,
    ))
    return key, value


def build_history(dicts):
    """Build a history structure"""
    dicts.sort(key=DATETIME_GETTER)
    unique_dicts = list(unique_justseen(dicts, key=make_squash_key))
    unique_dicts.reverse()
    latest, diffs_generator = dict_diff_incremental(unique_dicts)
    diffs = list(diffs_generator)
    return latest, diffs


def build_record(begin_ip_int, end_ip_int, dicts, existing=None):
    """Create database records for an iterable of merged dicts."""

    assert dicts or existing, "no data at all to pack?"

    if not dicts:
        # No new dicts; avoid expensive re-serialisation. Note that
        # blindly reusing the existing key/value pair from the database
        # (by not updating it at all) is not correct, the begin and end
        # of the range may have changed.
        return build_key_value(
            begin_ip_int,
            end_ip_int,
            existing.latest_json,
            existing.latest_datetime,
            existing.history_msgpack)

    if not existing:
        # Only new dicts, no existing data
        latest, diffs = build_history(dicts)
        return build_key_value(
            begin_ip_int,
            end_ip_int,
            json_dumps(latest, ensure_ascii=False).encode('UTF-8'),
            latest['datetime'],
            msgpack_dumps_utf8(diffs))

    # Merge new data and existing record
    dicts.extend(existing.iter_versions())
    latest, diffs = build_history(dicts)
    return build_key_value(
        begin_ip_int,
        end_ip_int,
        json_dumps(latest, ensure_ascii=False).encode('UTF-8'),
        latest['datetime'],
        msgpack_dumps_utf8(diffs))


class ExistingRecord(object):
    """Helper class for working with records retrieved from the database."""
    def __init__(self, key, value):
        # Performance note: except for the initial value unpacking, all
        # expensive deserialization operations are deferred until
        # requested.
        unpacked = msgpack_loads(value, use_list=False)

        # IP addresses
        self.begin_ip_packed = unpacked[0]
        self.end_ip_packed = key

        # Actual data, without any expensive decoding applied
        self.latest_json = unpacked[1]
        self.latest_datetime = unpacked[2].decode('ascii')
        self.history_msgpack = unpacked[3]

    def iter_versions(self, inplace=False):
        """Lazily reconstruct all versions in this record."""

        # Latest version
        latest = json_loads(self.latest_json)
        yield latest

        # Reconstruct history by applying patches incrementally
        yield from dict_patch_incremental(
            latest,
            msgpack_loads(
                self.history_msgpack,
                use_list=False,
                encoding='UTF-8'),
            inplace=inplace)


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
            record = ExistingRecord(key, value)
            yield (
                ip_packed_to_int(record.begin_ip_packed),
                ip_packed_to_int(record.end_ip_packed),
                record,
            )

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

            # Build and store a new record
            key, value = build_record(
                begin_ip_int,
                end_ip_int,
                items,
                existing)
            self.db.put(key, value)

            # Update counters
            n_processed += 1
            if existing is not None:
                n_updated += 1

        reporter.tick(True)

        logger.info("Compacting database... (this may take a while)")
        self.db.compact_range(start=b'\x00' * 16, stop=b'\xff' * 16)

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

        # Pack incoming IP address to a format suitable for lookups.
        ip_packed = ip_str_to_packed(ip)

        # Iterator construction is relatively costly, so reuse it for
        # performance reasons. The iterator won't see any data written
        # after its construction, but that is not a problem since the
        # data set is static.
        if self.iter is None:
            self.iter = self.db.iterator()

        # The database key stores the end IP of all ranges, so a simple
        # seek positions the iterator at the right key (if found).
        self.iter.seek(ip_packed)
        db_record = next(self.iter, None)

        # If the seek moved past the last range in the database: no hit
        if db_record is None:
            return None

        # Decode the value
        key, value = db_record
        record = ExistingRecord(key, value)

        # Check range boundaries. If the IP currently being looked up is
        # in a gap, there is no hit after all.
        if ip_packed < record.begin_ip_packed:
            return None

        # If the lookup is for the most recent version, we're done. No
        # decoding required.
        if datetime is None:
            return record.latest_json

        return_history = (datetime == 'all')

        # The most recent version may be the one asked for. No decoding
        # required in that case.
        if not return_history and record.latest_datetime <= datetime:
            return record.latest_json

        # Reconstruct complete history if the query asks for all
        # historical data. The JSON response is an object (not a list)
        # for security reasons.
        if return_history:
            return json_dumps(
                {'history': list(record.iter_versions())},
                ensure_ascii=False,
            ).encode('UTF-8')

        # This is a lookup for a specific timestamp. Iteratively
        # apply patches until (hopefully) a match is found.
        for d in record.iter_versions(inplace=True):
            if d['datetime'] <= datetime:
                return json_dumps(d, ensure_ascii=False).encode('UTF-8')

        # Too bad, no result
        return None
