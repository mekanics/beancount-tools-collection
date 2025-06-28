"""
Beancount importers for various financial institutions.

This module contains importers for Swiss and international financial institutions.
"""

# Import all available importers


try:
    from . import finpension
except ImportError:
    finpension = None

try:
    from . import ibkr
except ImportError:
    ibkr = None

try:
    from . import revolut
except ImportError:
    revolut = None



try:
    from . import viac
except ImportError:
    viac = None

try:
    from . import viseca
except ImportError:
    viseca = None

try:
    from . import yuh
except ImportError:
    yuh = None



try:
    from . import firefly_iii
except ImportError:
    firefly_iii = None

# List of available importers (only those that imported successfully)
__all__ = [name for name, module in locals().items() 
           if module is not None and not name.startswith('_')]

# Metadata
SUPPORTED_INSTITUTIONS = {
    'swiss': ['finpension', 'viac', 'viseca', 'yuh'],
    'international': ['ibkr', 'revolut'],
    'other': ['firefly_iii']
} 