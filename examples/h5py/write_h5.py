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


class H5Database(object):
    def __init__(self, filename, **kwargs):
        """Generic H5Database
        :param filename: database filename
        :type filename: str
        """
        self._filename = filename
        self._readonly = kwargs.get("readonly", True)
        self._swmr = kwargs.get("swmr", False)
        self._handle = None

    @classmethod
    def purge_root_item(cls, root_item):
        """Purge a root item string
        :param root_item: input root item
        :type root_item: str
        :return: purge root item, remove invalid keys like empty strings
        :rtype: [type]
        """

        if len(root_item) == 0:
            root_item = "/"
        if root_item == "//":
            root_item = "/"
        if root_item[0] != "/":
            root_item = "/" + root_item
        if root_item[-1] != "/":
            root_item = root_item + "/"
        if root_item == len(root_item) * root_item[0]:
            root_item = "/"
        return root_item

    def is_empty(self):
        """Checks if H5Database is empty
        :return: TRUE if is empty
        :rtype: bool
        """
        if self.is_open():
            return len(self.handle.keys()) == 0

    def initialize(self):
        """Initialize database"""
        if self.is_open():
            pass

    @classmethod
    def purge_key(cls, key):
        """Purges key if it is not compliat (i.e. if '/' is missing as first character)
        :param key: key to purge
        :type key: str
        :return: purged key
        :rtype: str
        """
        if not key.startswith("/"):
            return "/" + key
        return key

    @property
    def readonly(self):
        """Checks if database is in read only mode
        :return: TRUE for readonly mode
        :rtype: bool
        """
        return self._readonly

    @property
    def filename(self):
        """Linked filename
        :return: filename
        :rtype: str
        """
        return self._filename

    @property
    def handle(self) -> h5py.File:
        """Pointer to real h5py file
        :return: h5py database
        :rtype: h5py.File
        """
        return self._handle

    def is_open(self):
        """TRUE if file is already open"""

        return self.handle is not None

    def open(self):
        """Opens related file"""
        if not self.is_open():
            self._handle = h5py.File(
                self.filename, "r" if self.readonly else "a", swmr=self._swmr
            )
            if self.is_empty():
                self.initialize()

    def close(self):
        """Closes related file"""
        if self.is_open():
            self._handle.close()
            self._handle = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def get_group(self, key, force_create=True) -> h5py.Group:
        """Fetches a Group by key if any
        :param key: group key
        :type key: str
        :param force_create: TRUE to create Group if not present, defaults to True
        :type force_create: bool, optional
        :return: fetched Group if any
        :rtype: h5py.Group
        """
        group = None
        if self.is_open():
            if key not in self.handle:
                if force_create:
                    group = self.handle.create_group(key)
            else:
                group = self.handle[key]
        return group

    def get_data(self, group_key, name) -> h5py.Dataset:
        """Fetches Dataset if any
        :param group_key: group key
        :type group_key: str
        :param name: dataset name
        :type name: str
        :return: fetched Dataset if any
        :rtype: h5py.Dataset
        """
        data = None
        if self.is_open():
            group = self.get_group(key=group_key)
            if group is not None:
                if name in group:
                    data = group[name]
        else:
            pass  # TODO: logging when file is closed!
        return data

    def create_data(
        self,
        group_key,
        name,
        shape,
        dtype=None,
        maxshape=None,
        compression=H5DatasetCompressionMethods.NONE,
    ) -> h5py.Dataset:
        """Creates Dataset
        :param group_key: group key
        :type group_key: str
        :param name: dataset name
        :type name: str
        :param shape: dataset shape as np.darray shape
        :type shape: tuple
        :param dtype: data type, defaults to None
        :type dtype: numpy.dtype, optional
        :param maxshape: max allowed shape for resizable data, defaults to None
        :type maxshape: tuple, optional
        :param compression: compressione type
        :type compression: H5DatasetCompressionMethods type
        :raises Exception: Exception
        :return: created dataset
        :rtype: h5py.Dataset
        """
        data = None
        if self.is_open():
            group = self.get_group(group_key, force_create=True)
            data = group.create_dataset(
                name,
                shape=shape,
                maxshape=maxshape,
                dtype=dtype,
                compression=compression.name,
                compression_opts=compression.opts,
            )
        else:
            pass  # TODO: logging when file is closed!
        return data

    def store_object(self, group_key, name, obj) -> h5py.Dataset:
        """Stores generic object as dataset
        :param group_key: group key
        :type group_key: str
        :param name: data name
        :type name: str
        :param obj: object to store. numpy array or byte array
        :type obj: list / np.ndarray
        :return: created h5py.Dataset
        :rtype: h5py.Dataset
        """
        data = None
        if self.is_open():
            group = self.get_group(group_key, force_create=True)
            group[name] = obj
            data = group[name]
        return data

    def store_encoded_data(self, group_key, name, array, encoding):
        """Stores encoded data array
        :param group_key: group key
        :type group_key: str
        :param name: dataset name
        :type name: str
        :param array: plain array data
        :type array: list or np.ndarray
        :param encoding: encoder name [e.g. 'jpg']
        :type encoding: str
        """
        data = self.store_object(
            group_key,
            name,
            DataCoding.numpy_image_to_bytes_buffer(array, data_encoding=encoding),
        )
        if data is not None:
            data.attrs["_encoding"] = encoding

    def is_encoded_data(self, group_key, name) -> bool:
        """Checks if key/name is an encoded data database
        :param group_key: group key
        :type group_key: str
        :param name: database name
        :type name: str
        :return: TRUE if database contains encoded data (it uses attrs schema to understand it)
        :rtype: bool
        """
        return self.get_data_encoding(group_key, name) is not None

    def get_data_encoding(self, group_key, name) -> str:
        """Gets data encoding if any, otherwise returns None
        :param group_key: group key
        :type group_key: str
        :param name: database name
        :type name: str
        :return: encoding string if any, otherwise None
        :rtype: str
        """
        data = self.get_data(group_key, name)
        if data is not None:
            if "_encoding" in data.attrs:
                return data.attrs["_encoding"]
        return None

    def load_encoded_data(self, group_key, name):
        """Loads decoded data to array
        :param group_key: group key
        :type group_key: str
        :param name: dataset name
        :type name: str
        :raises AttributeError: Raises AttributeError if dataset doesn't have 'encodings' attributes
        :return: decoded data
        :rtype: object
        """
        data = self.get_data(group_key, name)
        if data is not None:
            encoding = self.get_data_encoding(group_key, name)
            if encoding is not None:
                decoded_data = DataCodec.decode_data_from_bytes(
                    data[...], encoding=encoding
                )
                return decoded_data
            else:
                raise AttributeError(
                    f"Encoding attribute is missing for {group_key}/{name}"
                )
        return None


class H5ToolKit:
    ENCODING_STRING = '_encoding'
    ENCODING_BINARY = 'pkl'
    OPTIONS = {
        'png': {'compress_level': 4}
    }

    @classmethod
    def get_encoding(cls, dataset: h5py.Dataset) -> Optional[str]:
        if cls.ENCODING_STRING in dataset.attrs:
            return dataset.attrs[cls.ENCODING_STRING]
        return None

    @classmethod
    def set_encoding(cls, dataset: h5py.Dataset, encoding: str):
        dataset.attrs[cls.ENCODING_STRING] = encoding

    @classmethod
    def decode_data(cls, dataset: h5py.Dataset):
        encoding = cls.get_encoding(dataset)
        data = None
        if encoding is not None:
            data = DataCoding.bytes_to_data(dataset[...], encoding)
            if data is None:
                data = pickle.loads(dataset[...])
        else:
            data = dataset[...]
        return data

    @classmethod
    def store_data(cls, group: h5py.Group, key: str, data: any, encoding: str = None):

        if encoding is None:
            if isinstance(data, np.ndarray):
                group[key] = data
            else:
                buffer = BytesIO(bytes())
                pickle.dump(data, buffer)
                group[key] = buffer.getbuffer()
                group[key].attrs[cls.ENCODING_STRING] = cls.ENCODING_BINARY

        elif DataCoding.is_image_extension(encoding):
            options = cls.OPTIONS.get(encoding, {})
            buffer = BytesIO(bytes())
            imageio.imwrite(buffer, data, format=encoding, **options)
            group[key] = buffer.getbuffer()
            group[key].attrs[cls.ENCODING_STRING] = encoding
        # else:
            # elif DataCoding.is_text_extension(encoding):
            #     buffer = BytesIO(bytes())
            #     np.savetxt(buffer, data)
            #     group[key] = buffer.getbuffer()
            # elif DataCoding.is_numpy_extension(encoding):
            #     buffer = BytesIO(bytes())
            #     np.save(buffer, data)
            #     group[key] = buffer.getbuffer()
            # elif DataCoding.is_metadata_extension(encoding):
            #     buffer = BytesIO(bytes())
            #     yaml.safe_dump(data, buffer, encoding='utf-8')
            #     group[key] = buffer.getbuffer()
            # elif DataCoding.is_pickle_extension(encoding):
            # buffer = BytesIO(bytes())
            # pickle.dump(data, buffer)
            # group[key] = buffer.getbuffer()
        else:
            raise NotImplementedError(f'Unknown encoding: {encoding}')

        if encoding is not None:
            group[key].attrs[cls.ENCODING_STRING] = encoding


class H5Item(MetaItem):
    def __init__(self, item: h5py.Dataset) -> None:
        super().__init__()
        self._item = item

    def source(self) -> any:
        return self._item


class H5Sample(Sample):
    def __init__(self, group: h5py.Group, lazy: bool = True, id: Hashable = None):
        """Creates a H5Sample based on a key/filename map

        :param group: h5py Group
        :type group: h5py.Group
        :param lazy: FALSE to preload data (slow), defaults to False
        :type lazy: bool, optional
        :param id: hashable value used as id
        :type id: Hashable, optional
        """
        super().__init__(id=id)
        self._group = group
        self._cached = {}
        if not lazy:
            for k in self.keys():
                d = self[k]

    def is_cached(self, key) -> bool:
        return key in self._cached

    def __getitem__(self, key):
        if not self.is_cached(key):
            dataset = self._group[key]
            data = H5ToolKit.decode_data(dataset)
            self._cached[key] = data
        return self._cached[key]

    def __setitem__(self, key, value):
        self._cached[key] = value

    def __delitem__(self, key):
        if key in self._cached:
            del self._cached[key]

    def __iter__(self):
        return iter(set.union(set(self._group.keys()), set(self._cached.keys())))

    def __len__(self):
        return len(self._group)

    def copy(self):
        newsample = H5Sample(self._group, id=self.id)
        newsample._cached = self._cached.copy()
        return newsample

    def rename(self, old_key: str, new_key: str):
        if new_key not in self._group and old_key in self._group:
            self._group[new_key] = self._group.pop(old_key)
            if old_key in self._cached:
                self._cached[new_key] = self._cached.pop(old_key)

    def metaitem(self, key: any):
        if key in self._group:
            return H5Item(self._filesmap[key])
        else:
            return MemoryItem()

    @property
    def skeleton(self) -> dict:
        return {x: None for x in self._group.keys()}

    def flush(self):
        keys = list(self._cached.keys())
        for k in keys:
            del self._cached[k]


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
    data = H5Database(filename=filename, readonly=False)
    data.open()

    extensions_map = {
        'image': 'bmp',
    }
    root_files_keys = {'shape', 'global_meta'}

    for idx, sample in enumerate(gen):
        key = f"/items/{idx}"
        group = data.get_group(key)
        for k, v in sample.items():
            if k not in root_files_keys:
                H5ToolKit.store_data(group, k, v, extensions_map.get(k, None))

    data.close()


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

    data = H5Database(filename=filename, readonly=True)
    data.open()
    root = data.handle["/items/"]
    for idx, sample in enumerate(root):
        group = root[sample]
        sample = H5Sample(group=group, id=idx)
        samples.append(sample)

    sequence = SamplesSequence(samples=samples)

    mixup = []
    for sample in sequence:
        for key, v in sample.items():
            # if 'pose' in key or 'shape' in key:
            #     print(key, v)
            # if key == 'jpg':
            #     cv2.imshow("jpg", v)
            #     cv2.waitKey(1)
            # if key == 'metadata':
            #     print("Metadata", v)
            mixup.append((key, v))
    print(len(mixup))

    # extensions_map = {
    #     'jpg': 'jpg',
    #     'png': 'png',
    #     'shape': 'txt',
    #     'shapen': 'txt',
    #     'global_meta': 'yml',
    #     'metadata': 'yml'
    # }
    # root_files_keys = {'shape', 'global_meta'}
    # zfill = 5

    # gen = samples_generator(size=size)
    # writer = UnderfolderWriter(
    #     folder='result',
    #     root_files_keys=root_files_keys,
    #     extensions_map=extensions_map,
    #     zfill=zfill
    # )

    # writer(sequence)


size = 100

t1 = time.perf_counter()

# -------------------------
# WRITE H5PY
filename = 'test.h5'
write_dataset_h5py(filename, size=size)

# -------------------------
# WRITE UNDERFOLDER
# filename = 'test_fs'
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
