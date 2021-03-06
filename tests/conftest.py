import yaml
from pipelime.tools.toydataset import ToyDatasetGenerator
import pytest
import os
from pathlib import Path


@pytest.fixture(scope='session')
def data_folder():
    dirname = os.path.dirname(__file__)
    return os.path.join(dirname, 'sample_data')


@pytest.fixture(scope='function')
def toy_dataset_small(tmpdir):
    folder = Path(tmpdir.mkdir("toy_dataset"))
    datafolder = folder / 'data'
    size = 32
    image_size = 256
    zfill = 5
    ToyDatasetGenerator.generate_toy_dataset(
        datafolder,
        size=size,
        image_size=image_size,
        zfill=zfill
    )

    global_meta = {
        'name': 'toy_dataset_small',
        'numbers': [1, 2, 3, 4, 5, 6]
    }

    global_meta_name = folder / 'global_meta.yml'
    yaml.safe_dump(global_meta, open(global_meta_name, 'w'))

    return {
        'folder': folder,
        'data_folder': datafolder,
        'size': size,
        'image_size': image_size,
        'zfill': zfill,
        'keypoints_format': 'xyas',
        'bboxes_format': 'pascal_voc',
        'expected_keys': ['image', 'mask', 'inst', 'keypoints', 'metadata', 'bboxes'],
        'root_keys': ['global_meta']
    }


@pytest.fixture(scope='session')
def filesystem_datasets(data_folder):
    return {
        'minimnist_underfolder': {
            'folder': Path(data_folder) / 'datasets' / 'underfolder_minimnist',
            'type': 'Undefolder'
        }
    }
