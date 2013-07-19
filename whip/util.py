
import heapq
import itertools
import operator
import socket
import struct
import subprocess

__all__ = [
    'int_to_ip',
    'merge_ranges',
    'open_file',
]


EVENT_TYPE_BEGIN = 0
EVENT_TYPE_END = 1


def int_to_ip(n, _ntoa=socket.inet_ntoa, _pack=struct.Struct('>L').pack):
    """Convert an integer into dot-decimal notation.

    This function converts a (max 32 bit) integer into the common
    notation for IP addresses. Example: ``0x01020304`` becomes
    *1.2.3.4*.
    """
    return _ntoa(_pack(n))


def merge_ranges(inputs):
    """
    Merge multiple ranges into a combined stream of ranges.

    This function combines all inputs by generating "change events" for
    each individual input, and merging these together in a single stream
    of changes. It then groups the change stream by position, and
    iterates over it, yielding a snapshot at each change point.
    """

    def generate_changes(it):
        for begin, end, data in it:
            assert begin <= end
            yield begin, EVENT_TYPE_BEGIN, data
            yield end + 1, EVENT_TYPE_END, data

    changes_generators = [generate_changes(input) for input in inputs]
    all_changes = heapq.merge(*changes_generators)
    grouper = itertools.groupby(all_changes, operator.itemgetter(0))
    active = set()
    previous_position = None

    for position, changes in grouper:

        # Yield output range from the previous position up to the
        # current position, containing all currently valid values.
        if active:
            yield previous_position, position - 1, sorted(active)

        # Apply begin/end changes
        for _, event_type, data in changes:
            if event_type == EVENT_TYPE_BEGIN:
                assert data not in active
                active.add(data)
            elif event_type == EVENT_TYPE_END:
                active.remove(data)

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
