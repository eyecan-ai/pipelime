import os
from pathlib import Path

import pytest
import yaml

from pipelime.tools.toydataset import ToyDatasetGenerator


@pytest.fixture(scope="session")
def data_folder():
    dirname = os.path.dirname(__file__)
    return os.path.join(dirname, "sample_data")


@pytest.fixture(scope="function")
def toy_dataset_small(tmpdir):
    folder = Path(tmpdir.mkdir("toy_dataset"))
    datafolder = folder / "data"
    size = 32
    image_size = 256
    zfill = 5
    ToyDatasetGenerator.generate_toy_dataset(
        datafolder, size=size, image_size=image_size, zfill=zfill
    )

    global_meta = {"name": "toy_dataset_small", "numbers": [1, 2, 3, 4, 5, 6]}

    global_meta_name = folder / "global_meta.yml"
    yaml.safe_dump(global_meta, open(global_meta_name, "w"))

    return {
        "folder": folder,
        "data_folder": datafolder,
        "size": size,
        "image_size": image_size,
        "zfill": zfill,
        "keypoints_format": "xyas",
        "bboxes_format": "pascal_voc",
        "expected_keys": ["image", "mask", "inst", "keypoints", "metadata", "bboxes"],
        "root_keys": ["global_meta"],
    }


@pytest.fixture(scope="function")
def toy_h5dataset_small(tmpdir):
    folder = Path(tmpdir.mkdir("toy_h5dataset"))
    datafolder = folder / "data"
    filename = folder / "h5dataset.h5"
    size = 32
    image_size = 256
    zfill = 5
    ToyDatasetGenerator.generate_toy_dataset(
        datafolder, size=size, image_size=image_size, zfill=zfill
    )

    global_meta = {"name": "toy_dataset_small", "numbers": [1, 2, 3, 4, 5, 6]}

    global_meta_name = folder / "global_meta.yml"
    yaml.safe_dump(global_meta, open(global_meta_name, "w"))

    return {
        "folder": folder,
        "data_folder": datafolder,
        "size": size,
        "image_size": image_size,
        "zfill": zfill,
        "keypoints_format": "xyas",
        "bboxes_format": "pascal_voc",
        "expected_keys": ["image", "mask", "inst", "keypoints", "metadata", "bboxes"],
        "root_keys": ["global_meta"],
    }


@pytest.fixture(scope="session")
def filesystem_datasets(data_folder):
    return {
        "minimnist_underfolder": {
            "folder": Path(data_folder) / "datasets" / "underfolder_minimnist",
            "type": "Undefolder",
            "schemas": {
                "simple": {
                    "filename": Path(data_folder)
                    / "datasets"
                    / "underfolder_minimnist_schemas"
                    / "simple.schema",
                    "valid": True,
                    "should_pass": True,
                },
                "deep": {
                    "filename": Path(data_folder)
                    / "datasets"
                    / "underfolder_minimnist_schemas"
                    / "deep.schema",
                    "valid": True,
                    "should_pass": True,
                },
                "invalid": {
                    "filename": Path(data_folder)
                    / "datasets"
                    / "underfolder_minimnist_schemas"
                    / "invalid.schema",
                    "valid": True,
                    "should_pass": False,
                },
                "bad_file": {
                    "filename": Path(data_folder)
                    / "datasets"
                    / "underfolder_minimnist_schemas"
                    / "bad.schema",
                    "valid": False,
                    "should_pass": False,
                },
            },
        },
        "empty_underfolder": {
            "folder": Path(data_folder) / "datasets" / "underfolder_empty",
            "type": "Undefolder",
            "schemas": {},
        }
    }


@pytest.fixture(scope="session")
def sample_underfolder_minimnist(filesystem_datasets):
    return filesystem_datasets["minimnist_underfolder"]


@pytest.fixture(scope="session")
def sample_underfolder_empty(filesystem_datasets):
    return filesystem_datasets["empty_underfolder"]


@pytest.fixture(scope="session")
def h5_datasets(data_folder):
    return {
        "minimnist_h5": {
            "filename": Path(data_folder) / "datasets" / "underfolder_minimnist.h5",
            "type": "H5",
            # 'schemas': {
            #     'simple': {
            #         'filename': Path(data_folder) / 'datasets' / 'underfolder_minimnist_schemas' / 'simple.schema',
            #         'valid': True,
            #         'should_pass': True
            #     },
            #     'deep': {
            #         'filename': Path(data_folder) / 'datasets' / 'underfolder_minimnist_schemas' / 'deep.schema',
            #         'valid': True,
            #         'should_pass': True
            #     },
            #     'invalid': {
            #         'filename': Path(data_folder) / 'datasets' / 'underfolder_minimnist_schemas' / 'invalid.schema',
            #         'valid': True,
            #         'should_pass': False
            #     },
            #     'bad_file': {
            #         'filename': Path(data_folder) / 'datasets' / 'underfolder_minimnist_schemas' / 'bad.schema',
            #         'valid': False,
            #         'should_pass': False
            #     }
            # }
        }
    }
