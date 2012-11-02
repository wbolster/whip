
"""
Pigeon, fast IP Geo lookup
"""

import csv
import logging
from math import ceil
import struct
import socket

import leveldb
import simplejson as json

__all__ = ['PigeonStore']

DEFAULT_DATABASE_DIR = 'db/'
logger = logging.getLogger(__name__)


#
# Utilities
#

IP_STRUCT = struct.Struct('>L')


def incr_ip(ip):
    n = IP_STRUCT.unpack(ip)[0] + 1
    try:
        return IP_STRUCT.pack(n)
    except struct.error:
        return None


#
# Public API
#

class PigeonStore(object):
    def __init__(self, database_dir=None):
        if database_dir is None:
            database_dir = DEFAULT_DATABASE_DIR
        logger.debug("Opening database %s", database_dir)
        self.db = leveldb.LevelDB(database_dir)

    def load(self, fp):
        """Load CSV data from an open file-like object"""
        put = self.db.Put
        dr = csv.DictReader(fp, delimiter='\t')

        inet_ntoa = socket.inet_ntoa
        dumps = json.dumps
        pack_ip = IP_STRUCT.pack

        for n, rec in enumerate(dr, 1):
            start_ip = pack_ip(int(rec['start_ip_int']))
            end_ip = pack_ip(int(rec['end_ip_int']))
            key = start_ip + end_ip

            # TODO: carrier_id, tld_id, sld_id, reg_org_id,
            # phone_number_prefix, asn, cidr

            tz = rec['timezone']
            if tz != '999':
                timezone = None
            else:
                timezone = '{:+04d}'.format(int(ceil(100 * float(tz))))

            value = dumps(dict(
                begin=inet_ntoa(start_ip),
                end=inet_ntoa(end_ip),
                continent=(rec['continent'], 1.),
                country=(rec['country_iso2'], float(rec['country_cf']) / 100),
                state=(rec['state'], float(rec['state_cf']) / 100),
                city=(rec['city'], float(rec['city_cf']) / 100),
                postal_code=rec['postal_code'],
                type=rec['connectiontype'],
                routing=rec['ip_routingtype'],
                coordinates=(float(rec['latitude']), float(rec['longitude'])),
                timezone=timezone,
                line_speed=rec['linespeed'],
                asn=rec['asn'],
            ))

            put(key, value)

            if n % 10000 == 0:
                logger.info('Indexed %d records', n)

    def lookup(self, ip):
        """Lookup a single ip address in the database"""
        iter_kwargs = dict(
            include_value=True,
            reverse=True)

        range_key = incr_ip(ip)
        if range_key is not None:
            iter_kwargs.update(key_to=range_key)

        it = self.db.RangeIter(**iter_kwargs)
        try:
            key, value = it.next()
        except StopIteration:
            # Start of range, no hit
            return None

        # Looked up ip must be within the range
        end = key[4:]
        if ip > end:
            return None

        return value
