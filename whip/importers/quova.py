# encoding: UTF-8
"""
Importer for Quova data sets.
"""

import collections
import csv
import itertools
import logging
import math

from whip.util import int_to_ip


logger = logging.getLogger(__name__)


# Header names used in the reference files
REF_HEADERS = frozenset(['carrier', 'org', 'sld', 'tld'])


# Description of all fields in the .dat files
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


def _clean(v):
    """Cleanup an input value"""
    if v in ('', 'unknown'):
        return None

    return v


def _parse_reference_set(fp):
    """Generator to parse a reference set into records"""
    ref_reader = csv.reader(fp, delimiter='|')

    for row in ref_reader:
        if not row[0] in REF_HEADERS:
            raise ValueError(
                "Unexpected input in reference data file: expected "
                "header line, got %r" % (row))

        ref_type, n, _ = row
        n = int(n)

        logger.info(
            "Reading %d reference records for type '%s'",
            n, ref_type)

        for ref_id, value in itertools.islice(ref_reader, n):
            yield ref_type, ref_id, value


class QuovaImporter(object):
    """Importer for Quova data sets."""

    def __init__(self, dir, tmp_db):
        self.dir = dir
        self.tmp_db = tmp_db

    def iter_records(self):

        logger.info("Populating temporary reference database")
        ref_db = self.tmp_db
        for ref_type, ref_id, value in _parse_reference_set(self.ref_fp):
            key = '{}-{}'.format(ref_type, ref_id)
            ref_db.put(key, value)

        logger.info("Reading data file")
        reader = csv.reader(self.data_fp, delimiter='|')
        it = (map(_clean, item) for item in reader)
        it = itertools.starmap(QuovaRecord, it)

        for record in it:

            begin_ip = int_to_ip(int(record.start_ip_int))
            end_ip = int_to_ip(int(record.end_ip_int))

            out = {
                # Network information
                'begin': begin_ip,
                'end': end_ip,
                'cidr': int(record.cidr),
                'connection_type': record.connectiontype,
                'line_speed': record.linespeed,
                'ip_routingtype': record.ip_routingtype,
                'asn': int(record.asn),

                # Network information (reference database lookups)
                'sld': _clean(ref_db.get('sld-' + record.sld_id)),
                'tld': _clean(ref_db.get('tld-' + record.tld_id)),
                'reg': _clean(ref_db.get('org-' + record.reg_org_id)),
                'carrier': _clean(ref_db.get('carrier-' + record.carrier_id)),

                # Geographic information
                'continent': record.continent,
                'country': record.country,
                'country_iso2': record.country_iso2,
                'country_cf': int(record.country_cf),
                'region': record.region,
                'state': record.state,
                'state_cf': int(record.state_cf),
                'city': record.city,
                'city_cf': int(record.city_cf),
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
