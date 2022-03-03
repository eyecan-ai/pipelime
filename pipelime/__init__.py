"""Top-level package for pipelime."""

__author__ = "eyecan"
__email__ = "daniele.degregorio@eyecan.ai"
__version__ = "0.1.4"


from pathlib import Path
from appdirs import AppDirs
import pipelime


def user_data_dir() -> Path:
    return Path(AppDirs(appname=pipelime.__name__, appauthor=__author__).user_data_dir)
