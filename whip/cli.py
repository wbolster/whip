"""
Whip command line interface module.
"""

# pylint: disable=missing-docstring

import argparse
import gzip
import io
import json
import logging
import os
import socket
import sys
import time

import aaargh

from .db import Database
from .reader import iter_json


logger = logging.getLogger(__name__)


def lookup_and_print(db, ip, dt):
    try:
        key = socket.inet_aton(ip)
    except OSError:
        print("Invalid IP address")
        return

    value = db.lookup(key, dt)
    if value is None:
        print("No hit found")
        return

    # Note: UltraJSON (ujson) does not support pretty printing, so use
    # built-in JSON module instead.
    parsed = json.loads(value.decode('UTF-8'))
    print(json.dumps(parsed, indent=2, sort_keys=True))


app = aaargh.App(description="Fast IP geo lookup")
app.arg('--db', default='db', dest='db_dir')


@app.cmd(name='load', help="Load data")
@app.cmd_arg('inputs', type=argparse.FileType('rb'), nargs='+')
def load_data(db_dir, inputs):

    logger.info(
        "Importing %d data files: %r",
        len(inputs), ', '.join(x.name for x in inputs))

    def gzip_wrap(fp):
        if fp.name.endswith('.gz'):
            return gzip.open(fp, mode='rt', encoding='UTF-8')
        else:
            return io.TextIOWrapper(fp, encoding='UTF-8')

    inputs = map(gzip_wrap, inputs)
    iters = map(iter_json, inputs)
    db = Database(db_dir, create_if_missing=True)
    db.load(*list(iters))


@app.cmd(name="lookup")
@app.cmd_arg('ips', help="The IP address(es) to lookup", nargs='+')
@app.cmd_arg('--datetime', '--dt', dest='dt')
def lookup(ips, db_dir, dt):
    db = Database(db_dir)
    for ip in ips:
        lookup_and_print(db, ip, dt)


@app.cmd(name="shell")
@app.cmd_arg('--datetime', '--dt', dest='dt')
def shell(db_dir, dt):
    db = Database(db_dir)
    try:
        while True:
            ip = input('IP: ')
            lookup_and_print(db, ip, dt)
    except EOFError:
        pass


@app.cmd(name='perftest', help="Run performance test")
@app.cmd_arg('--iterations', '-n', default=100 * 1000, type=int,
             help="The number of iterations")
@app.cmd_arg('--test-set', type=argparse.FileType('r'))
@app.cmd_arg('--datetime', '--dt', dest='dt')
def perftest(db_dir, iterations, test_set, dt):
    db = Database(db_dir)
    size = 4

    if test_set:
        logger.info("Using test set %r", test_set.name)
        it = (socket.inet_aton(line.strip()) for line in test_set)
    else:
        logger.info("Running %d iterations with random IP addresses",
                    iterations)
        # Use a sliding window over random data to obtain 4 bytes at a time
        rand_bytes = os.urandom(iterations + size - 1)
        it = (rand_bytes[n:n + size] for n in range(iterations))

    _lookup = db.lookup
    start_time = time.time()
    n = 0
    for n, ip in enumerate(it, 1):
        _lookup(ip, dt)

    elapsed = time.time() - start_time
    out = "{:d} lookups in {:.2f}s ({:.2f} req/s)".format(
        n, elapsed, n / elapsed)
    print(out)
    print('Cache statistics:', _lookup.cache_info())


@app.cmd
@app.cmd_arg('--host', default='0')
@app.cmd_arg('--port', type=int, default=5555)
def serve(host, port, db_dir):
    from .web import app as application
    application.config['DATABASE_DIR'] = db_dir
    application.run(host=host, port=port)


def main():
    logging.basicConfig(
        format='%(asctime)s (%(name)s) %(levelname)s: %(message)s',
        level=logging.INFO,
    )
    return app.run()


if __name__ == '__main__':
    sys.exit(main())
