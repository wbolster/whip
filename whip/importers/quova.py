# encoding: UTF-8
"""
Importer for Quova data sets.
"""

import collections
import csv
import datetime
import itertools
import logging
import math
import os
import re

from whip.util import ipv4_int_to_str, open_file


logger = logging.getLogger(__name__)


ISO8601_DATETIME_FMT = '%Y-%m-%dT%H:%M:%S'

# Regular expression to match file names like
# "EDITION_Gold_YYYY-MM-DD_vXXX.dat.gz"
DATA_FILE_RE = re.compile(r'''
    ^
    EDITION_Gold_
    (?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})  # date component
    _v(?P<version>\d+)  # version number
    \.dat\.gz  # file type
    $
    ''', re.VERBOSE)


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
        for ref_id, value in itertools.islice(ref_reader, int(n)):
            yield ref_type, ref_id, value


class QuovaImporter(object):
    """Importer for Quova data sets."""

    def __init__(self, dir, tmp_db):
        self.dir = dir
        self.tmp_db = tmp_db

    def iter_records(self):

        files = os.listdir(self.dir)
        data_files = [d for d in files if DATA_FILE_RE.match(d) is not None]
        if not data_files:
            raise RuntimeError(
                "No data file (.dat.gz) found in in directory %r"
                % dir)
        elif len(data_files) != 1:
            raise RuntimeError(
                "Multiple data files (.dat.gz) found in in directory %r"
                % dir)

        data_file = os.path.join(self.dir, data_files[0])
        logger.info("Found data file %r", data_file)

        match = DATA_FILE_RE.match(os.path.basename(data_file))
        match_dict = match.groupdict()
        version = int(match_dict['version'])
        dt = datetime.datetime(int(match_dict['year']),
                               int(match_dict['month']),
                               int(match_dict['day']))

        logger.info(
            "Detected date %s and version %d for data file %r",
            dt.strftime(ISO8601_DATETIME_FMT), version, data_file)

        reference_file = data_file.replace('.dat.gz', '.ref.gz')
        if not os.path.exists(reference_file):
            raise RuntimeError("Reference file %r not found" % reference_file)

        logger.info(
            "Building temporary reference database from %r",
            reference_file)

        ref_fp = open_file(reference_file)
        ref_db = self.tmp_db
        for ref_type, ref_id, value in _parse_reference_set(ref_fp):
            ref_db.put(ref_type + ref_id, value)

        logger.info("Reading data file %r", data_file)

        reader = csv.reader(open_file(data_file), delimiter='|')
        it = (map(_clean, item) for item in reader)
        it = itertools.starmap(QuovaRecord, it)

        for n, record in enumerate(it, 1):

            begin_ip = ipv4_int_to_str(int(record.start_ip_int))
            end_ip = ipv4_int_to_str(int(record.end_ip_int))

            out = {
                # Data file information
                'datetime': dt.strftime(ISO8601_DATETIME_FMT),

                # Network information
                'begin': begin_ip,
                'end': end_ip,
                'cidr': int(record.cidr),
                'connection_type': record.connectiontype,
                'line_speed': record.linespeed,
                'ip_routing_type': record.ip_routingtype,
                'asn': int(record.asn),

                # Network information (reference database lookups)
                'sld': _clean(ref_db.get('sld' + record.sld_id)),
                'tld': _clean(ref_db.get('tld' + record.tld_id)),
                'reg': _clean(ref_db.get('org' + record.reg_org_id)),
                'carrier': _clean(ref_db.get('carrier' + record.carrier_id)),

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

            if n % 100000 == 0:
                logger.info(
                    "Read %d records from %r; current position: %s",
                    n, data_file, begin_ip)

        logger.info("Finished reading %r (%d records)", data_file, n)
