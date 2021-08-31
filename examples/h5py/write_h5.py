from pipelime.sequences.readers.h5 import H5Reader
from pipelime.sequences.writers.h5 import H5Writer
from pipelime.h5.toolkit import H5ToolKit
from pipelime.sequences.readers.filesystem import UnderfolderReader
import time
import uuid
import json
from pipelime.sequences.writers.filesystem import UnderfolderWriter
import yaml
import imageio
import pickle
import sys
import albumentations as A
import cv2
from pipelime.tools.bytes import DataCoding
from typing import Hashable, Optional
from pipelime.sequences.samples import (
    FilesystemItem,
    MemoryItem,
    MetaItem,
    PlainSample,
    Sample,
    SamplesSequence,
)
import h5py
from pathlib import Path
from io import BytesIO
from types import SimpleNamespace
import numpy as np


class H5DatasetCompressionMethods(object):

    NONE = SimpleNamespace(name=None, opts=None)
    GZIP = SimpleNamespace(name="gzip", opts=4)


def samples_generator(size=5):
    for i in range(size):
        original_image = cv2.imread('/Users/danieledegregorio/Downloads/fake_picture.jpg')
        trans = A.Compose([
            A.ShiftScaleRotate(p=1.0),
            A.HueSaturationValue(hue_shift_limit=180, sat_shift_limit=50, val_shift_limit=50, p=1.0),
            A.HorizontalFlip(),
            A.VerticalFlip()
        ])
        out_image = trans(image=original_image)['image']

        yield PlainSample(data={
            'image': out_image,
            # 'png': out_image,
            'shape': out_image.shape,
            'shapen': np.array(out_image.shape),
            'metadata': {
                'sample_idx': i,
                'bool': True,
                'str': 'hello'
            },
            'global_meta': {
                'uuid': str(uuid.uuid1())
            },
            'camera': {
                'intrisics': np.array([1, 2, 3, 4, 5, 6]).reshape((2, 3))
            }

        }, id=i)


def write_dataset_fs(filename, size=100):

    extensions_map = {
        'image': 'bmp',
        'shape': 'txt',
        'shapen': 'txt',
        'global_meta': 'yml',
        'metadata': 'yml'
    }
    root_files_keys = {'shape', 'global_meta'}
    zfill = 5

    gen = samples_generator(size=size)
    writer = UnderfolderWriter(
        folder=filename,
        root_files_keys=root_files_keys,
        extensions_map=extensions_map,
        zfill=zfill
    )

    samples = []
    for idx, sample in enumerate(gen):
        samples.append(sample)

    sequence = SamplesSequence(samples=samples)
    writer(sequence)


def write_dataset_h5py(filename, size=100):
    gen = samples_generator(size=size)

    extensions_map = {
        'image': 'bmp',
    }
    root_files_keys = {'camera', 'global_meta'}
    zfill = 5

    writer = H5Writer(filename=filename, root_files_keys=root_files_keys, extensions_map=extensions_map)

    samples = []
    for idx, sample in enumerate(gen):
        samples.append(sample)

    sequence = SamplesSequence(samples=samples)
    writer(sequence)


def read_dataset_underfolder(filename):
    samples = []
    reader = UnderfolderReader(folder=filename)
    mixup = []
    for sample in reader:
        for key, v in sample.items():
            mixup.append((key, v))
    print(len(mixup))


def read_dataset_h5py(filename):
    samples = []

    reader = H5Reader(filename=filename)

    mixup = []
    for sample in reader:
        for key, v in sample.items():
            # if 'pose' in key or 'shape' in key:
            #     print(key, v)
            if key == 'image':
                print(v.shape)
                cv2.imshow("image", v)
                cv2.waitKey(1)
            # if key == 'metadata':
            #     print("Metadata", v)
            mixup.append((key, v))
    print(len(mixup))


size = 10000

t1 = time.perf_counter()

# -------------------------
# WRITE H5PY
filename = '/tmp/test.h5'
write_dataset_h5py(filename, size=size)

# -------------------------
# WRITE UNDERFOLDER
# filename = '/tmp/test_fs'
# write_dataset_fs(filename, size=size)

# -------------------------
# READ H5PY
# filename = 'test.h5'
# read_dataset_h5py(filename)

# -------------------------
# READ UNDERFODLER
# filename = 'test_fs'
# read_dataset_underfolder(filename)


t2 = time.perf_counter()
print("Time:", t2 - t1)
