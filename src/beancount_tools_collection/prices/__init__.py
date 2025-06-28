"""
Price fetchers for beancount.

This module contains price sources for automatically updating commodity prices.
"""

try:
    from . import ibkr
except ImportError:
    ibkr = None

# List of available price sources
__all__ = [name for name, module in locals().items() 
           if module is not None and not name.startswith('_')]

SUPPORTED_SOURCES = {
    'brokers': ['ibkr'],
    'web': [],
} 