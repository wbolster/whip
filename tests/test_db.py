
from socket import inet_aton
import tempfile

from whip.db import Database
from whip.json import loads as json_loads
from whip.util import ipv4_str_to_int


def test_db_loading():

    def t(begin, end, x, datetime):
        """Helper to create test data"""
        return dict(begin=begin, end=end, x=x, datetime=datetime)

    def iter_snapshot(snapshot):
        for d in snapshot:
            yield ipv4_str_to_int(d['begin']), ipv4_str_to_int(d['end']), d

    def lookup(db, ip, datetime=None):
        """Lookup a single version"""
        ret = db.lookup(inet_aton(ip), datetime=datetime) or b'{}'
        return json_loads(ret)

    def lookup_all_x(db, ip):
        """Lookup all versions, returning only the 'x' values"""
        history = lookup(db, ip, 'all')['history']
        return [d['x'] for d in history]

    snapshots = [
        [
            # Initial data
            t('1.0.0.0', '1.255.255.255', 1, '2010'),
            t('3.0.0.0', '3.255.255.255', 2, '2010'),
            t('8.0.0.0', '9.255.255.255', 3, '2010'),
        ],
        [
            # Split some ranges, exclude some ranges
            t('1.0.0.0', '1.2.3.4', 7, '2011'),
            t('1.2.3.5', '1.3.4.5', 8, '2011'),
        ],
        [
            # Merge some ranges, update some values
            t('1.0.0.0', '1.255.255.255', 4, '2013'),
            t('3.0.0.0', '3.255.255.255', 5, '2013'),
            t('8.0.0.0', '9.255.255.255', 6, '2013'),
        ],
    ]

    s1, s2, s3 = snapshots

    def check_sanity(*snapshots_lists):
        with tempfile.TemporaryDirectory() as db_dir:
            db = Database(db_dir, create_if_missing=True)

            for snapshots in snapshots_lists:
                iters = [iter_snapshot(s) for s in snapshots]
                db.load(*iters)

            # Latest version
            assert lookup(db, '1.0.0.0')['x'] == 4
            assert lookup(db, '1.255.255.255')['x'] == 4
            assert lookup(db, '7.0.0.0') == {}
            assert lookup(db, '8.1.2.3')['x'] == 6
            assert lookup(db, '12.0.0.0') == {}

            # Specific dates
            assert lookup(db, '1.2.3.3', '2010')['x'] == 1
            assert lookup(db, '1.2.3.4', '2011')['x'] == 7
            assert lookup(db, '1.2.3.5', '2011')['x'] == 8
            assert lookup(db, '1.100.100.100', '2011')['x'] == 1
            assert lookup(db, '8.1.2.3', '2011')['x'] == 3

            # No hit for really old dates
            assert lookup(db, '1.2.3.4', '2009') == {}

            # Future date
            assert lookup(db, '1.2.3.4', '2038')['x'] == 4

            # All versions
            assert lookup_all_x(db, '1.2.3.4') == [4, 7, 1]

    # Test loading in various combinations and check whether the
    # resulting database is correct.

    # All at once
    check_sanity(
        [s1, s2, s3]
    )

    # One at a time
    check_sanity(
        [s1],
        [s2],
        [s3],
    )

    # In chunks
    check_sanity(
        [s1, s2],
        [s3],
        [],
    )

    # Randomized with duplicate snapshots
    check_sanity(
        [s3, s2],
        [s2, s1],
        [s1],
    )
