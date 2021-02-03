import pytest
from pathlib import Path


@pytest.fixture(scope='session')
def augmentations_test_configurations(data_folder):
    configurations_folder = Path(data_folder) / 'augmentations'
    return [
        {
            'filename': configurations_folder / 'full_custom_augmentations.yml'
        },
    ]
