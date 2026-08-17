import importlib
import tomllib
from importlib import metadata
from pathlib import Path
from unittest.mock import patch

import beancount_tools_collection as pkg


def test_version_single_source():
    pyproject = tomllib.loads(Path('pyproject.toml').read_text())
    assert pkg.__version__ == pyproject['project']['version']


def test_version_falls_back_when_not_installed():
    with patch.object(metadata, 'version', side_effect=metadata.PackageNotFoundError):
        reloaded = importlib.reload(pkg)
        assert reloaded.__version__ == '0+unknown'
    importlib.reload(pkg)
