# encoding: UTF-8
"""
Importer for Quova data sets.
"""

# TODO: Use csv.DictReader instead and transform dict in-place?

import collections
import csv
import datetime
import itertools
import logging
import math
import os
import re

from whip.util import ipv4_int_to_str, open_file, PeriodicCallback

logger = logging.getLogger(__name__)

ISO8601_DATETIME_FMT = '%Y-%m-%dT%H:%M:%S'

# Regular expression to match file names. From the docs:
#
#    Data File V7 Naming Convention
#
#    Every file is named with information that qualifies the intended
#    recipient and data release information. The file name is named
#    using the following components:
#
#    <QuovaNet_customer_id>_v<data_version>_<internal_id>_<yyyymmdd>.csv.gz
#
#    For example, a file created from release version 470.63, production
#    job 15.27, on May 25, 2010 for customer quova would have the name:
#    quova_v470.63_15.27_20100525.gz
#
# However, in reality, the suffix is '.csv.gz', not '.gz'.
#
DATA_FILE_RE = re.compile(r'''
    ^
    (?P<customer_id>.+)
    _v(?P<version>.+)
    _(?P<internal_id>.+)
    _(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})
    \.csv(:?\.gz)?
    $
    ''', re.VERBOSE)


# Numeric fields
NUMERIC_FIELDS = frozenset(('as', 'country_cf', 'state_cf', 'city_cf'))

# Description of all fields in the .dat files
QuovaRecord = collections.namedtuple('QuovaRecord', (
    'start_ip_int',
    'end_ip_int',
    'continent',
    'country',
    'country_code',
    'country_cf',
    'region',
    'state',
    'state_code',
    'state_cf',
    'city',
    'city_cf',
    'postal_code',
    'area_code',
    'time_zone',
    'latitude',
    'longitude',
    'dma',
    'msa',
    'connection_type',
    'line_speed',
    'ip_routing_type',
    'asn',
    'sld',
    'tld',
    'organization',
    'carrier',
    'anonymizer_status',
))


def clean(v):
    """Cleanup an input value"""
    # TODO: is 'unknown' still used in v7?
    if v in ('', 'unknown'):
        return None

    return v


class QuovaImporter(object):
    """Importer for Quova data sets."""

    def __init__(self, data_file):
        self.data_file = data_file

    def iter_records(self):
        data_file = self.data_file
        logger.info("Using data file %r", data_file)

        match = DATA_FILE_RE.match(os.path.basename(data_file))
        if not match:
            raise RuntimeError(
                "Unrecognized data file name: %r (is it the correct file?)"
                % data_file)

        match_dict = match.groupdict()
        version = int(match_dict['version'])
        dt = datetime.datetime(int(match_dict['year']),
                               int(match_dict['month']),
                               int(match_dict['day']))
        dt_as_str = dt.strftime(ISO8601_DATETIME_FMT)

        logger.info(
            "Detected date %s and version %d for data file %r",
            dt_as_str, version, data_file)

        # Open CSV data file, clean each field in each input row, and
        # construct a QuovaRecord.
        fp = open_file(data_file)
        reader = csv.reader(fp)
        it = iter(reader)
        next(it)  # skip header line
        it = (map(clean, item) for item in it)
        it = itertools.starmap(QuovaRecord, it)

        reporter = PeriodicCallback(lambda: logger.info(
            "Read %d records from %r; current position: %s",
            n, data_file, ipv4_int_to_str(begin_ip_int)))

        for n, record in enumerate(it, 1):

            begin_ip_int = int(record.start_ip_int)
            end_ip_int = int(record.end_ip_int)

            out = {
                # Data file information
                'datetime': dt_as_str,

                # Network information
                'begin': ipv4_int_to_str(begin_ip_int),
                'end': ipv4_int_to_str(end_ip_int),
                'connection_type': record.connection_type,
                'line_speed': record.line_speed,
                'ip_routing_type': record.ip_routing_type,
                'as': record.asn,
                'sld': record.sld,
                'tld': record.tld,
                'organization': record.organization,
                'carrier': record.carrier,
                'anonymizer_status': record.anonymizer_status,

                # Geographic information
                'continent': record.continent,
                'country': record.country,
                'country_code': record.country_code,
                'country_cf': record.country_cf,
                'region': record.region,
                'state': record.state,
                'state_code': record.state_code,
                'state_cf': record.state_cf,
                'city': record.city,
                'city_cf': record.city_cf,
                'postal_code': record.postal_code,
                'area_code': record.area_code,
                'latitude': float(record.latitude),
                'longitude': float(record.longitude),
            }

            # Convert numeric fields, but only if set
            for key in NUMERIC_FIELDS:
                if out[key] is not None:
                    out[key] = int(out[key])

            if record.time_zone == '999':  # FIXME: is this still used in v7?
                out['time_zone'] = None
            else:
                # Convert time zone information into ±HH:MM format
                tz_f, hours = math.modf(float(record.time_zone))
                minutes = abs(60 * tz_f)
                out['time_zone'] = '%+03d:%02d' % (hours, minutes)

            yield begin_ip_int, end_ip_int, out

            reporter.tick()

        reporter.tick(True)
        logger.info("Finished reading %r (%d records)", data_file, n)
