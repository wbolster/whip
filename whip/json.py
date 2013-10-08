"""
JSON compatibility module.

This module use the fastest JSON implementation available.
"""

import importlib

for lib in ('ujson', 'simplejson', 'json'):
    try:
        json = importlib.import_module(lib)
        break
    except ImportError:
        pass


__all__ = ['dump', 'dumps', 'load', 'loads']

dump = json.dump
dumps = json.dumps
load = json.load
loads = json.loads
