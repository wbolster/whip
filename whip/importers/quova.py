# encoding: UTF-8

"""
Importer for Quova data sets.
"""

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
INTEGER_FIELDS = frozenset(('asn', 'country_cf', 'state_cf', 'city_cf'))
FLOAT_FIELDS = frozenset(('latitude', 'longitude'))
IGNORED_FIELDS = frozenset(('dma', 'msa'))


def clean_field(v):
    return None if v == '' else v


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
        version = match_dict['version']
        dt = datetime.datetime(int(match_dict['year']),
                               int(match_dict['month']),
                               int(match_dict['day']))
        dt_as_str = dt.strftime(ISO8601_DATETIME_FMT)

        logger.info(
            "Detected date %s and version %s for data file %r",
            dt_as_str, version, data_file)

        # Prepare for reading the CSV data
        fp = open_file(data_file)
        reader = csv.reader(fp)
        it = iter(reader)

        # Skip header line, but make sure it is actually a header line
        header_line = next(it)
        if header_line[0] != FIELDS[0]:
            raise ValueError(
                "First line of input file %r does not seem a header line"
                % data_file)

        reporter = PeriodicCallback(lambda: logger.info(
            "Read %d records from %r; current position: %s",
            n, data_file, ipv4_int_to_str(begin_ip_int)))

        izip = itertools.izip
        for n, record in enumerate(it, 1):

            out = dict(izip(FIELDS, map(clean_field, record)))

            # Data file information
            out['datetime'] = dt_as_str

            # Drop unwanted fields
            for k in IGNORED_FIELDS:
                del out[k]

            # Network information
            begin_ip_int = int(out.pop('start_ip_int'))
            end_ip_int = int(out.pop('end_ip_int'))
            out['begin'] = ipv4_int_to_str(begin_ip_int)
            out['end'] = ipv4_int_to_str(end_ip_int)

            # Convert numeric fields (if not None)
            for key in INTEGER_FIELDS:
                if out[key] is not None:
                    out[key] = int(out[key])
            for key in FLOAT_FIELDS:
                if out[key] is not None:
                    out[key] = float(out[key])

            # Convert time zone string like '-3.5' into ±HH:MM format
            if out['time_zone'] is not None:
                tz_frac, tz_int = math.modf(float(out['time_zone']))
                out['time_zone'] = '%+03d:%02d' % (tz_int, abs(60 * tz_frac))

            yield begin_ip_int, end_ip_int, out

            if n % 100 == 0:
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


def clean_field_old_format(v):
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
            refs[ref_type][int(ref_id)] = clean_field_old_format(value)

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
    n = 0
    for n, record in enumerate(reader, 1):
        for k, v in record.iteritems():
            record[k] = clean_field_old_format(v)

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

    logger.info("Saved %d records to %r", n, out_fp.name)
