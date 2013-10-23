"""
Main module, to make "python -m whip" work.
"""

import sys
from .cli import main

sys.exit(main())
