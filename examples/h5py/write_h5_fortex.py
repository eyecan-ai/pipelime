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


# underfolder = UnderfolderReader(
#     folder='/Users/danieledegregorio/work/workspace_eyecan/pipelime/tests/sample_data/datasets/underfolder_minimnist')

# filename = '/Users/danieledegregorio/work/workspace_eyecan/pipelime/tests/sample_data/datasets/underfolder_minimnist.h5'

# print("ROOT", underfolder.get_filesystem_template().root_files_keys)
# writer = H5Writer(
#     filename=filename,
#     root_files_keys=underfolder.get_filesystem_template().root_files_keys,
#     extensions_map={
#         'image_mask': 'png',
#         'image_maskinv': 'png',
#         'image': 'jpg',

#     },
#     zfill=underfolder.best_zfill()
# )

# writer(underfolder)

# reader = H5Reader(filename='/Users/danieledegregorio/work/workspace_eyecan/pipelime/tests/sample_data/datasets/underfolder_minimnist.h5')

# # print(reader.get_h5_template().root_files_keys)

# for sample in reader:
#     print(sample['metadata'])
#     image = sample['image']
#     cv2.imshow("image", image)
#     cv2.waitKey(1)
