"""
Beancount Tools Collection

A comprehensive collection of beancount tools including importers, price fetchers,
plugins, and utilities for financial institutions worldwide.
"""

from importlib.metadata import PackageNotFoundError, version

from . import importers, plugins, prices, scripts, utils

try:
    __version__ = version('beancount-tools-collection')
except PackageNotFoundError:  # source checkout, not installed
    __version__ = '0+unknown'

__author__ = 'Beancount Tools Collection Contributors'

__all__ = ['importers', 'prices', 'plugins', 'scripts', 'utils']
