
from socket import inet_aton
import tempfile

from whip.db import Database
from whip.json import loads as json_loads
from whip.util import ipv4_str_to_int


def test_db_loading():

    snapshot_1 = [
        dict(begin='1.0.0.0', end='1.255.255.255', x=1, datetime='2010'),
        dict(begin='3.0.0.0', end='3.255.255.255', x=2, datetime='2010'),
        dict(begin='8.0.0.0', end='9.255.255.255', x=3, datetime='2010'),
    ]

    snapshot_2 = [
        dict(begin='1.0.0.0', end='1.255.255.255', x=4, datetime='2011'),
        dict(begin='3.0.0.0', end='3.255.255.255', x=5, datetime='2011'),
        dict(begin='8.0.0.0', end='9.255.255.255', x=6, datetime='2011'),
    ]

    snapshots = [
        snapshot_1,
        snapshot_2,
    ]

    def iter_snapshot(snapshot):
        for d in snapshot:
            yield ipv4_str_to_int(d['begin']), ipv4_str_to_int(d['end']), d

    with tempfile.TemporaryDirectory() as db_dir:
        db = Database(db_dir, create_if_missing=True)
        iters = [iter_snapshot(s) for s in snapshots]
        db.load(*iters)

        def lookup(ip, datetime=None):
            ret = db.lookup(inet_aton(ip), datetime=datetime) or b'{}'
            return json_loads(ret)

        # Latest version
        assert lookup('1.0.0.0')['x'] == 4
        assert lookup('1.255.255.255')['x'] == 4
        assert lookup('7.0.0.0') == {}
        assert lookup('8.1.2.3')['x'] == 6
        assert lookup('12.0.0.0') == {}

        # Older date
        assert lookup('1.2.3.3', '2010')['x'] == 1

        # No hit for really old dates
        assert lookup('1.2.3.4', '2009') == {}

        # Future date
        assert lookup('1.2.3.4', '2038')['x'] == 4

        # All versions
        assert [d['x'] for d in lookup('1.2.3.4', 'all')['history']] == [4, 1]
