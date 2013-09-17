"""
Whip reader module.
"""

from whip.json import loads
from whip.util import ipv4_str_to_int

DEFAULT_RANGE_FIELDS = ('begin', 'end')


def iter_json(fp, range_fields=DEFAULT_RANGE_FIELDS):
    """Read a JSON formatted stream of data from a file like object.

    Each line in the input file must contain a valid JSON document. The
    fields specified by `range_fields` should contain IPv4 addresses
    that are used as the beginning and end of the range.

    The input must already be sorted by IP range, and the ranges must
    not overlap.

    """

    begin_field, end_field = range_fields
    _ipv4_str_to_int = ipv4_str_to_int
    _loads = loads

    for line in fp:
        doc = _loads(line)
        yield (
            _ipv4_str_to_int(doc[begin_field]),
            _ipv4_str_to_int(doc[end_field]),
            doc,
        )
