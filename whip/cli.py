
import argparse
import gzip
import json
import logging
import os
import socket
import sys
import time

import aaargh

from whip.db import Database
from whip.reader import iter_json


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

    # XXX: UltraJSON (ujson) does not support pretty printing, so
    # use built-in JSON module instead.
    parsed = json.loads(value.decode('UTF-8'))
    print(json.dumps(parsed, indent=2, sort_keys=True))


app = aaargh.App(description="Fast IP geo lookup")
app.arg('--db', default='db', dest='db_dir')


@app.cmd(name='load', help="Load data")
@app.cmd_arg('inputs', type=argparse.FileType('r'), nargs='+')
def load_data(db_dir, inputs):

    logger.info(
        "Importing %d data files: %r",
        len(inputs), ', '.join(x.name for x in inputs))

    def gzip_wrap(fp):
        if fp.name.endswith('.gz'):
            return gzip.GzipFile(mode='r', fileobj=fp)
        else:
            return fp

    inputs = map(gzip_wrap, inputs)
    iters = map(iter_json, inputs)
    db = Database(db_dir, create_if_missing=True)
    db.load(*iters)


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
        it = map(str.strip, test_set)
        it = map(socket.inet_aton, it)
    else:
        logger.info("Running %d iterations with random IP addresses",
                    iterations)
        # Use a sliding window over random data to obtain 4 bytes at a time
        rand_bytes = os.urandom(iterations + size - 1)
        it = (rand_bytes[n:n + size] for n in range(iterations))

    lookup = db.lookup
    start_time = time.time()
    for n, ip in enumerate(it, 1):
        lookup(ip, dt)

    elapsed = time.time() - start_time
    out = "{:d} lookups in {:.2f}s ({:.2f} req/s)".format(
        n, elapsed, n / elapsed)
    print(out)


@app.cmd
@app.cmd_arg('--host', default='0')
@app.cmd_arg('--port', type=int, default=5555)
def serve(host, port, db_dir):
    from whip.web import app
    app.config['DATABASE_DIR'] = db_dir
    app.run(host=host, port=port)


def main():
    logging.basicConfig(
        format='%(asctime)s (%(name)s) %(levelname)s: %(message)s',
        level=logging.INFO,
    )
    return app.run()


if __name__ == '__main__':
    sys.exit(main())
