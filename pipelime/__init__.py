"""Top-level package for pipelime."""

__author__ = """daniele de gregorio"""
__email__ = 'daniele.degregorio@eyecan.ai'
__version__ = '0.0.1'


# Import transforms in order to register classes for serialization
from pipelime.augmentations.transforms import PadIfNeededV2
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.writers.filesystem import UnderfolderWriter
