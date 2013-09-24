"""
JSON compatibility module.

Use the fastest JSON implementation available and export dump and load
function.
"""

for lib in ('ujson', 'simplejson', 'json'):
    try:
        json = __import__(lib)
        break
    except ImportError:
        pass


__all__ = ['dumps', 'loads']

dump = json.dump
dumps = json.dumps
load = json.load
loads = json.loads
