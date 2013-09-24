"""
Whip WSGI application module.
"""

# pylint: disable=pointless-statement

from .web import app as application

application  # makes flake8 happy
