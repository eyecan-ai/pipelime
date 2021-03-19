"""Top-level package for pipelime."""

__author__ = "eyecan"
__email__ = 'daniele.degregorio@eyecan.ai'
__version__ = '0.0.1'


# Import transforms in order to register classes for serialization
from pathlib import Path
from pipelime.augmentations.transforms import PadIfNeededV2
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.writers.filesystem import UnderfolderWriter
from appdirs import AppDirs
import pipelime


def user_data_dir() -> Path:
    # TODO: esurface and eyecan: move strings as package dunder
    return Path(AppDirs(appname=pipelime.__name__, appauthor=__author__).user_data_dir)
