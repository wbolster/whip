"""
Importer for Quova data sets.
"""

import collections
import csv
import itertools
import logging
import math

from whip.util import int_to_ip, open_file

logger = logging.getLogger(__name__)

QuovaRecord = collections.namedtuple('QuovaRecord', (
    'start_ip_int',
    'end_ip_int',
    'cidr',
    'continent',
    'country',
    'country_iso2',
    'country_cf',
    'region',
    'state',
    'state_cf',
    'city',
    'city_cf',
    'postal_code',
    'phone_number_prefix',
    'timezone',
    'latitude',
    'longitude',
    'dma',
    'msa',
    'pmsa',
    'connectiontype',
    'linespeed',
    'ip_routingtype',
    'aol',
    'asn',
    'sld_id',
    'tld_id',
    'reg_org_id',
    'carrier_id',
))


class QuovaImporter(object):
    def __init__(self, filename):
        logger.info("Opening file %r", filename)
        self.fp = open_file(filename)

    def iter_records(self):
        reader = csv.reader(self.fp, delimiter='|')
        records = itertools.starmap(QuovaRecord, reader)

        for record in records:

            begin_ip = int_to_ip(int(record.start_ip_int))
            end_ip = int_to_ip(int(record.end_ip_int))

            out = {
                # Network information
                # TODO: the *_id fields should be looked up in the
                # reference tables
                'begin': begin_ip,
                'end': end_ip,
                'cidr': int(record.cidr),
                'connection_type': record.connectiontype,
                'line_speed': record.linespeed,
                'ip_routingtype': record.ip_routingtype,
                'asn': int(record.asn),
                'sld_id': record.sld_id,
                'tld_id': record.tld_id,
                'reg_org_id': record.reg_org_id,
                'carrier_id': record.carrier_id,

                # Geographic information
                'continent': record.continent,
                'country': record.country,
                'country_iso2': record.country_iso2,
                'country_cf': record.country_cf,
                'region': record.region,
                'state': record.state,
                'state_cf': record.state_cf,
                'city': record.city,
                'city_cf': record.city_cf,
                'postal_code': record.postal_code,
                'phone_number_prefix': record.phone_number_prefix,
                'latitude': float(record.latitude),
                'longitude': float(record.longitude),
            }

            # Convert time zone information into ±HH:MM format
            if record.timezone == '999':
                out['timezone'] = None
            else:
                tz = float(record.timezone)
                hours = int(tz)
                minutes = 60 * (tz - math.floor(tz))
                out['timezone'] = '%+03d:%02d' % (hours, minutes)

            yield begin_ip, end_ip, out


if __name__ == '__main__':
    import json
    import sys

    filename = sys.argv[1]
    importer = QuovaImporter(filename)
    for begin, end, record in importer.iter_records():
        print(json.dumps(record))
