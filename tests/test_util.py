
from nose.tools import assert_equal, assert_list_equal

from whip.util import (
    ipv4_int_to_str,
    ipv4_str_to_int,
    ipv4_int_to_bytes,
    ipv4_bytes_to_int,
    merge_ranges,
)


def test_ipv4_conversion():

    items = [
        (123, '0.0.0.123', '\x00\x00\x00\x7b'),
        (1234567890, '73.150.2.210', '\x49\x96\x02\xd2'),
        (0x010203ff, '1.2.3.255', '\x01\x02\x03\xff'),
        (0x10111213, '16.17.18.19', '\x10\x11\x12\x13'),
    ]

    for as_int, as_human, as_bytes in items:
        assert_equal(ipv4_int_to_str(as_int), as_human)
        assert_equal(ipv4_int_to_bytes(as_int), as_bytes)
        assert_equal(ipv4_str_to_int(as_human), as_int)
        assert_equal(ipv4_bytes_to_int(as_bytes), as_int)


def test_merge_ranges():

    #
    # position   00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20
    #            ==============================================================
    # input a    a1-a1       a2    a3                               a4-------a4
    # input b       b1-------b1
    # input c    c1 c2-c2                            c3----c3
    # input d                                  d1----------------------------d1
    # input e
    #            ==============================================================
    # combined   z1 z2 z3 z4 z5    z6          z7-z7 z8----z8 z9-z9 z10-----z10
    #

    inputs = [
        [(0, 1, 'a1'), (4, 4, 'a2'), (6, 6, 'a3'), (17, 20, 'a4')],  # input a
        [(1, 4, 'b1')],                                              # input b
        [(0, 0, 'c1'), (1, 2, 'c2'), (12, 14, 'c3')],                # input c
        [(10, 20, 'd1')],                                            # input d
        [],                                                          # input e
    ]

    expected = [
        (0, 0,   ['a1', 'c1']),        # z1
        (1, 1,   ['a1', 'b1', 'c2']),  # z2
        (2, 2,   ['b1', 'c2']),        # z3
        (3, 3,   ['b1']),              # z4
        (4, 4,   ['a2', 'b1']),        # z5
        (6, 6,   ['a3']),              # z6
        (10, 11, ['d1']),              # z7
        (12, 14, ['c3', 'd1']),        # z8
        (15, 16, ['d1']),              # z9
        (17, 20, ['a4', 'd1']),        # z10
    ]

    # Sort data before comparing
    actual = [(b, e, sorted(data)) for b, e, data in merge_ranges(*inputs)]
    assert_list_equal(actual, expected)
