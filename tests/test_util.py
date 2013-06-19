
from nose.tools import assert_list_equal

from whip.util import merge_ranges


def test_merge_ranges():

    #
    # position:  1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0
    #            =======================================
    # input a:   111     2     3                 4444444
    # input b:   1111111
    # input c:   1 222                 33333
    # input d:                     111111111111111111111
    # input e:
    #            =======================================
    # combined:  Q R S T U     V   WWW XXXXX YYY ZZZZZZZ
    #

    inputs = [
        [(0, 1, 'a1'), (4, 4, 'a2'), (6, 6, 'a3'), (17, 20, 'a4')],
        [(1, 4, 'b1')],
        [(0, 0, 'c1'), (1, 2, 'c2'), (12, 14, 'c3')],
        [(10, 20, 'd1')],
        [],
    ]

    expected = [
        (0, 0, ['a1', 'c1']),
        (1, 1, ['a1', 'b1', 'c2']),
        (2, 2, ['b1', 'c2']),
        (3, 3, ['b1']),
        (4, 4, ['a2', 'b1']),
        (6, 6, ['a3']),
        (10, 11, ['d1']),
        (12, 14, ['c3', 'd1']),
        (15, 16, ['d1']),
        (17, 20, ['a4', 'd1']),
    ]

    assert_list_equal(
        list(merge_ranges(inputs)),
        expected,
    )
