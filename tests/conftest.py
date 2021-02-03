import pytest
import os
from pathlib import Path


@pytest.fixture(scope='session')
def data_folder():
    dirname = os.path.dirname(__file__)
    return os.path.join(dirname, 'sample_data')
