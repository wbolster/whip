#!/usr/bin/env python

# TODO: optionally include anonymous proxies data

import argparse
import csv
import gzip
import itertools
import signal
import sys


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
    with ref_fp:
        ref_reader = csv.reader(ref_fp, delimiter='|')

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
    for record in reader:
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

        # Write output
        writer.writerow(record)


def main():
    parser = argparse.ArgumentParser(
        description="Convert an older Quova data set into V7 format")
    parser.add_argument('data', type=gzip.open)
    parser.add_argument('reference', type=gzip.open)
    parser.add_argument('--output', '-o', type=argparse.FileType('w'),
                        nargs='?', default=sys.stdout)
    args = parser.parse_args()

    # Don't raise IOError messages on broken pipes, e.g. when using
    # "... | head"
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

    convert_to_v7(args.data, args.reference, args.output)


if __name__ == '__main__':
    sys.exit(main())
