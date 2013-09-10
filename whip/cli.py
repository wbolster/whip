import gzip
import itertools
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
    value = db.lookup(socket.inet_aton(ip), dt)
    if value is None:
        print("No hit found")
    else:
        # XXX: UltraJSON (ujson) does not support pretty printing, so
        # use built-in JSON module instead.
        print(json.dumps(json.loads(value), indent=2, sort_keys=True))


app = aaargh.App(description="Fast IP geo lookup")
app.arg('--database-dir', '--db', default='db')


@app.cmd(name='load', help="Load data")
@app.cmd_arg('inputs', type=file, nargs='+')
def load_data(database_dir, inputs):

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
    db = Database(database_dir, create_if_missing=True)
    db.load(*iters)


@app.cmd(name="lookup")
@app.cmd_arg('ips', help="The IP address(es) to lookup", nargs='+')
@app.cmd_arg('--datetime', '--dt', dest='dt')
def lookup(ips, database_dir, dt):
    db = Database(database_dir)
    for ip in ips:
        lookup_and_print(db, ip, dt)


@app.cmd(name="shell")
@app.cmd_arg('--datetime', '--dt', dest='dt')
def shell(database_dir, dt):
    db = Database(database_dir)
    while True:
        ip = raw_input('IP: ')
        lookup_and_print(db, ip, dt)


@app.cmd(name='perftest', help="Run performance test")
@app.cmd_arg('--iterations', '-n', default=100 * 1000, type=int,
             help="The number of iterations")
@app.cmd_arg('--test-set', type=file)
@app.cmd_arg('--datetime', '--dt', dest='dt')
def perftest(database_dir, iterations, test_set, dt):
    db = Database(database_dir)
    size = 4

    if test_set:
        logger.info("Using test set %r", test_set.name)
        it = itertools.imap(str.strip, test_set)
        it = itertools.imap(socket.inet_aton, it)
    else:
        logger.info("Running %d iterations with random IP addresses",
                    iterations)
        # Use a sliding window over random data to obtain 4 bytes at a time
        rand_bytes = os.urandom(iterations + size - 1)
        it = (rand_bytes[n:n + size] for n in xrange(iterations))

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
def serve(host, port, database_dir):
    from whip.web import app
    app.config['DATABASE_DIR'] = database_dir
    app.run(host=host, port=port)


def main():
    logging.basicConfig(
        format='%(asctime)s (%(name)s) %(levelname)s: %(message)s',
        level=logging.INFO,
    )
    return app.run()


if __name__ == '__main__':
    sys.exit(main())
