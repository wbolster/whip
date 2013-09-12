
import heapq
import itertools
import operator
import socket
import struct
import subprocess
import time


__all__ = [
    'ipv4_int_to_str',
    'ipv4_str_to_int',
    'ipv4_int_to_bytes',
    'ipv4_bytes_to_int',
    'merge_ranges',
    'open_file',
    'PeriodicCallback',
]

EVENT_TYPE_BEGIN = 0
EVENT_TYPE_END = 1

IPV4_STRUCT = struct.Struct('>L')

DEFAULT_CALLBACK_INTERVAL = 10


def ipv4_int_to_str(n, _inet_ntoa=socket.inet_ntoa, _pack=IPV4_STRUCT.pack):
    """Convert an integer into an IPv4 address string.

    This function converts a (max 32 bit) integer into the common
    dot-decimal notation for IP addresses. Example: ``0x01020304``
    becomes *1.2.3.4*.
    """
    return _inet_ntoa(_pack(n))


def ipv4_str_to_int(s, _inet_aton=socket.inet_aton,
                    _unpack=IPV4_STRUCT.unpack):
    """Convert an IPv4 address string into an integer.

    This is the reverse of :py:func:`ipv4_int_to_str`.
    """
    return _unpack(_inet_aton(s))[0]


def ipv4_bytes_to_int(b, _unpack=IPV4_STRUCT.unpack):
    """Convert a 4 byte string into an integer"""
    return _unpack(b)[0]


def ipv4_int_to_bytes(n, _pack=IPV4_STRUCT.pack):
    """Convert an integer into 4 byte string"""
    return _pack(n)


def merge_ranges(*inputs):
    """
    Merge multiple ranges into a combined stream of ranges.

    This function combines all inputs by generating "change events" for
    each individual input, and merging these together in a single stream
    of changes. It then groups the change stream by position, and
    iterates over it, yielding a snapshot at each change point.
    """

    def generate_changes(it, input_id):
        for begin, end, data in it:
            assert begin <= end
            yield begin, EVENT_TYPE_BEGIN, input_id, data
            yield end + 1, EVENT_TYPE_END, input_id, None

    changes_generators = [
        generate_changes(input, input_id)
        for input_id, input in enumerate(inputs)
    ]
    all_changes = heapq.merge(*changes_generators)
    grouper = itertools.groupby(all_changes, operator.itemgetter(0))
    active = {}
    previous_position = None

    for position, changes in grouper:

        # Yield output range from the previous position up to the
        # current position, containing all currently valid values.
        if active:
            yield previous_position, position - 1, active.values()

        # Apply begin/end changes
        for _, event_type, input_id, data in changes:
            if event_type == EVENT_TYPE_BEGIN:
                assert input_id not in active
                active[input_id] = data
            elif event_type == EVENT_TYPE_END:
                del active[input_id]

        # Remember current position
        previous_position = position

    # After consuming all changes, all ranges must be closed.
    assert len(active) == 0


def open_file(filename):
    """Open a file, transparently decompressing it if possible.

    Supported file name suffixes are '.gz', '.bz2', and '.xz'.

    This function uses an external process for decompression. This is
    often faster than using built-in modules, and also takes advantage
    of multiple CPU cores.
    """

    args = None
    if filename.endswith('.gz'):
        args = ['zcat', filename]
    elif filename.endswith('.bz2'):
        args = ['bzcat', filename]
    elif filename.endswith('.xz'):
        args = ['xzcat', filename]

    if args:
        proc = subprocess.Popen(args, stdout=subprocess.PIPE)
        fp = proc.stdout
    else:
        fp = open(filename)

    return fp


def dict_diff(d, base):
    """
    Calculate differences between a dict and a base dict.

    The return value is a tuple with two items: a `to_set` dictionary
    with all items in `d` that were either unset or changed, and
    a `to_delete` tuple with all removed keys.

    See also dict_patch().
    """
    # The loop below is equivalent to
    #
    #   to_set = dict(d.viewitems() - base.viewitems())
    #
    # ...but the loop below is more performant for dicts with more than
    # a few keys.
    to_set = {}
    for k, v in d.iteritems():
        if not k in base:
            to_set[k] = d[k]  # addition
        elif base[k] != v:
            to_set[k] = v  # mutation

    to_delete = tuple(base.viewkeys() - d.viewkeys())
    return to_set, to_delete


def dict_patch(d, to_set, to_delete):
    """
    Patches a dictionary using a changes dict and a list of deletions.

    This is the reverse of dict_diff().
    """
    d.update(to_set)
    for k in to_delete:
        del d[k]


class PeriodicCallback(object):
    def __init__(self, cb, interval=DEFAULT_CALLBACK_INTERVAL):
        assert callable(cb)
        self._cb = cb
        self._interval = interval
        self._last_report = float('-inf')

    def tick(self, force_report=False):
        if force_report or time.time() - self._last_report > self._interval:
            self._cb()
            self._last_report = time.time()
