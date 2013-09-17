
# Use fastest JSON implementation available
for lib in ('ujson', 'simplejson', 'json'):
    try:
        json = __import__(lib)
        break
    except ImportError:
        pass


__all__ = ['dumps', 'loads']

dumps = json.dumps
loads = json.loads
