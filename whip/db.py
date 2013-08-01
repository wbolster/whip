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
import simplejson as json

from whip.util import dict_diff, dict_patch, ipv4_int_to_bytes, merge_ranges

SIZE_STRUCT = struct.Struct('>H')

logger = logging.getLogger(__name__)

json_encoder = json.JSONEncoder(
    ensure_ascii=True,
    check_circular=False,
    separators=(',', ':'),  # no whitespace
)


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

        extract_datetime = operator.itemgetter('datetime')
        _encode = json_encoder.encode

        # Merge all iterables to produce unique, non-overlapping IP
        # ranges with multiple timestamped infosets.
        merged = merge_ranges(*iters)

        for n, item in enumerate(merged, 1):
            begin_ip_int, end_ip_int, infosets = item

            # Build history structure
            infosets.sort(key=extract_datetime, reverse=True)
            latest = infosets[0]
            latest_json = _encode(latest)
            history_json = _encode([
                dict_diff(d, latest)
                for d in infosets[1:]
            ])

            # Store data
            key = ipv4_int_to_bytes(end_ip_int)
            value = (ipv4_int_to_bytes(begin_ip_int)
                     + SIZE_STRUCT.pack(len(latest_json))
                     + latest_json
                     + history_json)

            # Store in database
            self.db.put(key, value)

            if n % 100000 == 0:
                logger.info('%d records stored', n)

        # Refresh iterator so that it sees the new data
        self._make_iter()

    def lookup(self, ip, dt=None, _unpack=SIZE_STRUCT.unpack,
               _decode=json.JSONDecoder().decode,
               _encode=json_encoder.encode):
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
        size = _unpack(value[4:6])[0]
        latest_json = value[6:size + 6]

        # If the lookup is for the recent version requested, we're done
        if dt is None:
            return latest_json

        # TODO: store latest date somewhere to avoid JSON parsing
        # overhead.

        # This is a historical lookup. This means we actually need to
        # peek into the record.
        latest = _decode(latest_json)

        # The most recent version may be the one asked for.
        if latest['datetime'] <= dt:
            return latest_json

        # Too bad, we need to delve deeper into history.
        history = _decode(value[size + 6:])
        for to_delete, to_set in history:
            if to_delete['datetime'] <= dt:
                dict_patch(latest, to_delete, to_set)
                return _encode(latest)

        # Too bad, no result
        return None
