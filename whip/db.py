
import logging
import struct
import socket

import plyvel
import simplejson as json


logger = logging.getLogger(__name__)

IP_STRUCT = struct.Struct('>L')


def incr_ip(ip, _unpack=IP_STRUCT.unpack, _pack=IP_STRUCT.pack):
    try:
        return _pack(_unpack(ip)[0] + 1)
    except struct.error:
        return None


class Database(object):
    def __init__(self, database_dir, create_if_missing=False):
        logger.debug("Opening database %s", database_dir)
        self.db = plyvel.DB(
            database_dir,
            create_if_missing=create_if_missing,
            lru_cache_size=128 * 1024 * 1024)
        self._make_iter()

    def _make_iter(self):
        """Make an iterator for the current database.

        Iterator construction is relatively costly, so reuse it for
        performance reasons. The iterator won't see any data written
        after its construction, but this is not a problem since the data
        set is static.
        """
        self.iter = self.db.iterator(reverse=True)

    def load(self, it):
        """Load data from an importer iterable"""
        for n, item in enumerate(it, 1):
            begin_ip, end_ip, data = item

            # The start IP is the key; the end IP is prepended to the value
            key = socket.inet_aton(begin_ip)
            value = socket.inet_aton(end_ip) + json.dumps(data)
            self.db.put(key, value)

            if n % 100000 == 0:
                logger.info('Merged %d records', n)

        # Refresh iterator so that it sees the new data
        self._make_iter()

    def lookup(self, ip):
        """Lookup a single ip address in the database"""
        range_key = incr_ip(ip)
        self.iter.seek(range_key)
        try:
            key, value = next(self.iter)
        except StopIteration:
            # Start of range, no hit
            return None

        # Looked up ip must be within the range
        end = value[:4]
        if ip > end:
            return None

        return value[4:]
