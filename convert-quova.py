#!/usr/bin/env python

# TODO: optionally include anonymous proxies data

import argparse
import csv
import gzip
import itertools
import logging
import os
import re
import signal
import sys

logger = logging.getLogger(__name__)

REF_HEADERS = frozenset(['carrier', 'org', 'sld', 'tld'])

IN_FIELDS = (
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

IN_NUMERICAL_FIELDS = [
    'asn',
    'city_cf',
    'country_cf',
    'dma',
    'msa',
    'phone_number_prefix',
    'postal_code',
    'state_cf',
]

OUT_FIELDS = (
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

RENAMED_FIELDS = {
    'connectiontype': 'connection_type',
    'country_iso2': 'country_code',
    'ip_routingtype': 'ip_routing_type',
    'linespeed': 'line_speed',
    'phone_number_prefix': 'area_code',
    'timezone': 'time_zone',
}


EMPTY_VALUES = frozenset(('', 'unknown', 'none'))


def clean(v):
    """Cleanup an input value"""
    if v in EMPTY_VALUES:
        return None

    return v


def convert_to_v7(data_fp, ref_fp, out_fp):

    # Reference data (lookups)
    refs = {
        'carrier': {},
        'org': {},
        'sld': {},
        'tld': {},
    }
    ref_reader = csv.reader(ref_fp, delimiter='|')

    logger.info("Reading reference file %r...", ref_fp.name)
    for row in ref_reader:
        if not row[0] in REF_HEADERS:
            raise ValueError(
                "Unexpected input in reference data file: expected "
                "header line, got %r" % (row))

        ref_type, n_records, max_id = row
        for ref_id, value in itertools.islice(ref_reader, int(n_records)):
            refs[ref_type][int(ref_id)] = clean(value)

    # Prepare reader
    reader = csv.DictReader(data_fp, IN_FIELDS, delimiter='|')

    # Prepare writer
    writer = csv.DictWriter(
        out_fp, OUT_FIELDS,
        delimiter=',',
        quoting=csv.QUOTE_ALL,
        quotechar='"',
        lineterminator='\n',
        extrasaction='ignore',
    )
    writer.writeheader()

    # Loop and transform
    logger.info("Transforming data file %r...", data_fp.name)
    for n, record in enumerate(reader, 1):
        for k, v in record.iteritems():
            record[k] = clean(v)

        # Replace missing numerical values by empty strings
        for key in IN_NUMERICAL_FIELDS:
            if record[key] == '0':
                record[key] = ''

        # Rename some fields
        for k1, k2 in RENAMED_FIELDS.iteritems():
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


def main():
    parser = argparse.ArgumentParser(
        description="Convert an older Quova data set into V7 format")
    parser.add_argument('data_file')
    parser.add_argument('--output-directory', '-o', required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    # Don't raise IOError messages on broken pipes, e.g. when using
    # "... | head"
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

    filename_regex = re.compile(
        r'''
        ^
        EDITION_Gold_
        (?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})
        _v(?P<version>.+)
        \.dat\.gz
        $
        ''',
        re.VERBOSE)
    m = re.match(filename_regex, os.path.basename(args.data_file))
    if not m:
        logger.error(
            "Input file name %r does not match expected file name format",
            args.data_file)
        sys.exit(1)

    fmt = 'converted_v{version}_dummy_{year}{month}{day}.csv.gz'
    output_filename = fmt.format(**m.groupdict())

    output_path = os.path.join(args.output_directory, output_filename)
    reference_file = args.data_file.replace('.dat', '.ref')

    logger.info(
        "Converting input files %r and %r to output file %r",
        args.data_file, reference_file, output_path)

    data_fp = gzip.open(args.data_file, 'r')
    reference_fp = gzip.open(reference_file, 'r')
    output_fp = gzip.open(output_path, 'w')
    convert_to_v7(data_fp, reference_fp, output_fp)


if __name__ == '__main__':
    sys.exit(main())
