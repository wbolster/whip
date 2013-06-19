
import csv
import logging
import math
import struct
import socket

import plyvel
import simplejson as json


logger = logging.getLogger(__name__)

IP_STRUCT = struct.Struct('>L')


def incr_ip(ip):
    n = IP_STRUCT.unpack(ip)[0] + 1
    try:
        return IP_STRUCT.pack(n)
    except struct.error:
        return None


def transform_record(rec):
    start_ip = IP_STRUCT.pack(int(rec['start_ip_int']))
    end_ip = IP_STRUCT.pack(int(rec['end_ip_int']))

    # TODO: carrier_id, tld_id, sld_id, reg_org_id,
    # phone_number_prefix, asn, cidr

    tz = rec['timezone']
    if tz != '999':
        timezone = None
    else:
        timezone = '{:+04d}'.format(int(math.ceil(100 * float(tz))))

    value = json.dumps(dict(
        begin=socket.inet_ntoa(start_ip),
        end=socket.inet_ntoa(end_ip),
        continent=(rec['continent'], 1.),
        country=(rec['country_iso2'],
                 float(rec['country_cf']) / 100),
        state=(rec['state'],
               float(rec['state_cf']) / 100),
        city=(rec['city'],
              float(rec['city_cf']) / 100),
        postal_code=rec['postal_code'],
        type=rec['connectiontype'],
        routing=rec['ip_routingtype'],
        coordinates=(float(rec['latitude']),
                     float(rec['longitude'])),
        timezone=timezone,
        line_speed=rec['linespeed'],
        asn=rec['asn'],
    ))

    # The start IP is the key; the end IP is prepended to the value
    key = start_ip
    value = end_ip + value

    return key, value


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

    def load(self, fp):
        """Load CSV data from an open file-like object"""
        dr = csv.DictReader(fp, delimiter='\t')
        put = self.db.put
        _transform = transform_record
        for n, rec in enumerate(dr, 1):
            key, value = _transform(rec)
            put(key, value)

            if n % 100000 == 0:
                logger.info('Indexed %d records', n)

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
