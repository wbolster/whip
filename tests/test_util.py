
from nose.tools import (
    assert_dict_equal,
    assert_equal,
    assert_list_equal,
)

from whip.util import (
    dict_diff,
    dict_diff_incremental,
    dict_patch,
    dict_patch_incremental,
    ip_int_to_packed,
    ip_int_to_str,
    ip_packed_to_int,
    ip_packed_to_str,
    ip_str_to_int,
    ip_str_to_packed,
    merge_ranges,
)


def test_ip_conversion():

    items = [

        # IPv4
        (
            0xffff00000102,
            '0.0.1.2',
            bytes.fromhex('0000 0000 0000 0000 0000 ffff 0000 0102'),
        ), (
            0xffff499602d2,
            '73.150.2.210',
            bytes.fromhex('0000 0000 0000 0000 0000 ffff 4996 02d2'),
        ), (
            0xffff010203ff,
            '1.2.3.255',
            bytes.fromhex('0000 0000 0000 0000 0000 ffff 0102 03ff'),
        ), (
            0xffff10111213,
            '16.17.18.19',
            bytes.fromhex('0000 0000 0000 0000 0000 ffff 1011 1213'),
        ),

        # IPv6
        (
            0x0102030405060708090a0b0c0d0e0faa,
            '102:304:506:708:90a:b0c:d0e:faa',
            bytes.fromhex('0102 0304 0506 0708 090a 0b0c 0d0e 0faa'),
        ), (
            0x20010db885a3004210008a2e03707334,
            '2001:db8:85a3:42:1000:8a2e:370:7334',
            bytes.fromhex('2001 0db8 85a3 0042 1000 8a2e 0370 7334'),
        ), (
            0x20010db8000000000000ff0000428329,
            '2001:db8::ff00:42:8329',
            bytes.fromhex('2001 0db8 0000 0000 0000 ff00 0042 8329'),
        ),
    ]

    for as_int, as_str, as_packed in items:
        assert_equal(ip_int_to_str(as_int), as_str)
        assert_equal(ip_int_to_packed(as_int), as_packed)
        assert_equal(ip_str_to_int(as_str), as_int)
        assert_equal(ip_packed_to_int(as_packed), as_int)
        assert_equal(ip_str_to_packed(as_str), as_packed)
        assert_equal(ip_packed_to_str(as_packed), as_str)


def test_merge_ranges():

    # Single input
    input = [
        (0, 1, 'a1'),
        (4, 4, 'a2'),
        (6, 6, 'a3'),
        (17, 20, 'a4'),
    ]
    expected = [
        (0, 1, ['a1']),
        (4, 4, ['a2']),
        (6, 6, ['a3']),
        (17, 20, ['a4']),
    ]
    actual = list(merge_ranges(input))
    assert_list_equal(actual, expected)

    # Multiple inputs:
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


def test_dict_patching():

    base = dict(a=1, b=2, c=3, d=4)

    inputs = [
        (dict(a=1, b=2, c=3, d=4),
         {},
         ()),

        ({},
         {},
         ('a', 'b', 'c', 'd')),

        (dict(a=1, b=2, c=3, d=5),
         dict(d=5),
         ()),

        (dict(a=4, b=3, c=2),
         dict(a=4, b=3, c=2),
         ('d')),

        (dict(a=1, b=2, e=5),
         dict(e=5),
         ('c', 'd')),
    ]

    for d, expected_modifications, expected_deletions in inputs:
        patch = dict_diff(base, d)
        assert_dict_equal(patch.modifications, expected_modifications)
        assert_list_equal(sorted(patch.deletions), sorted(expected_deletions))

        recreated = dict_patch(base, patch)
        assert_dict_equal(d, recreated)


def test_incremental_patching():
    inputs = [
        dict(a=1, b=1, c=1),
        dict(a=2, b=2),
        dict(a=3, b=3, c=3),
        dict(a=4, b=3, c=3),
    ]
    base, patches = dict_diff_incremental(inputs)
    patches = list(patches)

    assert len(patches) == len(inputs) - 1  # n inputs, n-1 diffs
    assert_dict_equal(base, inputs[0])

    reconstructed = list(dict_patch_incremental(base, patches))
    assert_list_equal(inputs, [base] + list(reconstructed))
