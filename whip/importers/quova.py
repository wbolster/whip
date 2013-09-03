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
FIELDS = (
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
)

# Structure for input records
QuovaRecord = collections.namedtuple('QuovaRecord', FIELDS)


def clean(v):
    """Cleanup an input value"""
    # TODO: is 'unknown' still used in v7?
    if v in ('', 'unknown'):
        return None

    return v



#
# Importer
#

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


#
# Old data format conversion
#

OLD_FORMAT_REF_HEADERS = frozenset(['carrier', 'org', 'sld', 'tld'])

OLD_FORMAT_FIELDS = (
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
)

OLD_FORMAT_NUMERICAL_FIELDS = [
    'asn',
    'city_cf',
    'country_cf',
    'dma',
    'msa',
    'phone_number_prefix',
    'postal_code',
    'state_cf',
]

OLD_FORMAT_RENAMED_FIELDS = {
    'connectiontype': 'connection_type',
    'country_iso2': 'country_code',
    'ip_routingtype': 'ip_routing_type',
    'linespeed': 'line_speed',
    'phone_number_prefix': 'area_code',
    'timezone': 'time_zone',
}

OLD_FORMAT_EMPTY_VALUES = frozenset(('', 'unknown', 'none'))


def clean_old_format(v):
    """Clean an input field (old data format)"""
    if v in OLD_FORMAT_EMPTY_VALUES:
        return None

    return v


def convert_to_v7(data_fp, ref_fp, out_fp):
    """Convert old format data and reference files to the V7 format"""

    # TODO: optionally include anonymous proxies data

    # Reference data (lookups)
    refs = {
        'carrier': {},
        'org': {},
        'sld': {},
        'tld': {},
    }
    ref_reader = csv.reader(ref_fp, delimiter='|')

    logger.info("Reading reference file %r", ref_fp.name)
    for row in ref_reader:
        if not row[0] in OLD_FORMAT_REF_HEADERS:
            raise ValueError(
                "Unexpected input in reference data file: expected "
                "header line, got %r" % (row))

        ref_type, n_records, max_id = row
        for ref_id, value in itertools.islice(ref_reader, int(n_records)):
            refs[ref_type][int(ref_id)] = clean_old_format(value)

    # Prepare reader
    reader = csv.DictReader(data_fp, OLD_FORMAT_FIELDS, delimiter='|')

    # Prepare writer
    writer = csv.DictWriter(
        out_fp, FIELDS,
        delimiter=',',
        quoting=csv.QUOTE_ALL,
        quotechar='"',
        lineterminator='\n',
        extrasaction='ignore',
    )
    writer.writeheader()

    # Loop and transform
    logger.info("Transforming data file %r", data_fp.name)
    for n, record in enumerate(reader, 1):
        for k, v in record.iteritems():
            record[k] = clean_old_format(v)

        # Replace missing numerical values by empty strings
        for key in OLD_FORMAT_NUMERICAL_FIELDS:
            if record[key] == '0':
                record[key] = ''

        # Rename some fields
        for k1, k2 in OLD_FORMAT_RENAMED_FIELDS.iteritems():
            record[k2] = record.pop(k1)

        # Reference lookups
        record['carrier'] = refs['carrier'][int(record['carrier_id'])]
        record['organization'] = refs['org'][int(record['reg_org_id'])]
        record['sld'] = refs['sld'][int(record['sld_id'])]
        record['tld'] = refs['tld'][int(record['tld_id'])]

        # Drop magic "empty" value for time zones
        if record['time_zone'] == '999':
            record['time_zone'] = ''

        # Write output
        writer.writerow(record)

        if n % 100000 == 0:
            logger.info("Converted %d input records", n)
