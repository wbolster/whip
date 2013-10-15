"""
Whip utility module.
"""

import collections
import heapq
import itertools
import operator
import socket
import struct
import time


#
# IP address conversion utilities
#

IPV4_STRUCT = struct.Struct('>L')


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


#
# Range merging
#

EVENT_TYPE_BEGIN = 0
EVENT_TYPE_END = 1


def merge_ranges(*inputs):
    """
    Merge multiple ranges into a combined stream of ranges.

    This function combines all inputs by generating "change events" for
    each individual input, and merging these together in a single stream
    of changes. It then groups the change stream by position, and
    iterates over it, yielding a snapshot at each change point.
    """

    # Shortcut in case there is only a single input iterable
    if len(inputs) == 1:
        for begin, end, data in inputs[0]:
            yield begin, end, [data]
        return

    def generate_change_events(it, input_id):
        """Generate start/stop "edges" for an iterable"""
        previous_end = -1
        for begin, end, data in it:
            # Assert that the input data is well-formed: the begin of
            # the range must be before the end, and each subsequent
            # range must be past the previous one.
            assert begin <= end
            assert begin > previous_end
            yield begin, EVENT_TYPE_BEGIN, input_id, data
            yield end + 1, EVENT_TYPE_END, input_id, None
            previous_end = end

    changes_generators = [
        generate_change_events(input, input_id)
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
            yield previous_position, position - 1, list(active.values())

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


#
# Dict diffing and squashing
#

DictPatch = collections.namedtuple(
    'DictPatch',
    ['modifications', 'deletions'])


def dict_diff(d1, d2):
    """
    Calculate differences between dicts `d1` and `d2`.

    The return value is a `dict patch` structure containing the
    differences.

    Note: this function does *not* recursively compare dicts.

    See also dict_patch().
    """
    return DictPatch(
        {k: v for k, v in d2.items() if k not in d1 or d1[k] != v},
        [k for k in d1 if k not in d2],
    )


def dict_patch(d, patch, *, inplace=False):
    """
    Apply a `dict patch` to a dict.

    If `inplace` is set to `True`, the dict `d` is modified in place;
    the default is to make a (shallow) copy before any changes are
    applied.

    This is the reverse of dict_diff().
    """
    if not inplace:
        d = d.copy()

    modifications, deletions = patch
    d.update(modifications)
    for k in deletions:
        del d[k]

    return d


def dict_diff_incremental(iterable):
    """
    Create incremental diffs for an iterable of dicts.

    The first dict in `iterable` (which must yield at least one dict)
    will be used as the base dict, and subsequent dicts will be returned
    as incremental patches. These incremental patches can be used to
    reconstruct the original dicts by incrementally applying those
    patches to the base dict (and its patched versions).

    This function returns a 2-tuple containing the base dict and
    a generator that yields incremental patches.

    Example: for the input `[d1, d2, d3, d4]`, this function returns the
    2-tuple `(d1, <patches-generator>)`; the generator yields 3 patches:
    `<diff from d1 to d2>, <diff from d2 to d3>, <diff from d3 to d4>`.
    """
    # This implementation is inspired by the pairwise() recipe from the
    # itertools documentation.
    a, b = itertools.tee(iterable)
    base = next(b)
    return base, itertools.starmap(dict_diff, zip(a, b))


def dict_patch_incremental(d, patches, *, inplace=False):
    """
    Apply incremental dict patches and yield each intermediate result.

    This is the reverse of dict_diff_incremental().
    """
    for patch in patches:
        d = dict_patch(d, patch, inplace=inplace)
        yield d


def unique_justseen(iterable, key=None):
    """
    List unique elements, preserving order.

    This is a copy/paste from a recipe in the itertools docs.
    """
    return map(next, map(operator.itemgetter(1),
                         itertools.groupby(iterable, key)))


#
# Progress logging
#

DEFAULT_CALLBACK_INTERVAL = 10


class PeriodicCallback(object):
    """
    Periodic callback ticker, useful for logging progress information.

    This class doesn't do anything but keeping a simple timer and
    a threshold. It's up to the calling code to call `tick()` everytime
    it may want the callback to be run.
    """

    def __init__(self, cb, interval=DEFAULT_CALLBACK_INTERVAL):
        assert callable(cb)
        self._cb = cb
        self._interval = interval
        self._last_report = float('-inf')

    def tick(self, force_report=False):
        """
        Ping this periodic callback instance.

        The callback function will be called if if enough time has
        elapsed.
        """
        if force_report or time.time() - self._last_report > self._interval:
            self._cb()
            self._last_report = time.time()
