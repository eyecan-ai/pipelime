import pickle
from io import BytesIO
from typing import Optional

import h5py
import imageio
import numpy as np

from pipelime.tools.bytes import DataCoding


class H5Database(object):
    ITEMS_BRANCH_NAME = "items"
    GLOBAL_BRANCH_NAME = "globals"

    def __init__(self, filename, **kwargs):
        """Generic H5Database
        :param filename: database filename
        :type filename: str
        """
        self._filename = filename
        self._readonly = kwargs.get("readonly", True)
        self._swmr = kwargs.get("swmr", True)
        self._handle = None

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
                self.filename,
                "r" if self.readonly else "a",
                swmr=(self._swmr and self.readonly),
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

    def get_sample_group(self, key: str, force_create: bool = True) -> h5py.Group:
        """Fetches a Sample Group by key. It pre-prend 'items' branch name to desired key
        to compose the fulle key, like: /<ITEM_BRANCH_NAME>/key

        :param key: sample key
        :type key: str
        :param force_create: True to create group if not present, defaults to True
        :type force_create: bool, optional
        :return: fetched Group if any
        :rtype: h5py.Group
        """

        group_key = f"/{self.ITEMS_BRANCH_NAME}/{key}"
        return self.get_group(group_key, force_create=force_create)

    def get_sample_root(self, force_create: bool = True) -> h5py.Group:
        """Fetches the Sample Group root

        :param force_create: True to create group if not present, defaults to True
        :type force_create: bool, optional
        :return: fetched Group if any
        :rtype: h5py.Group
        """
        return self.get_group(f"/{self.ITEMS_BRANCH_NAME}/", force_create=force_create)

    def get_global_group_name(self, key: str) -> str:
        return f"/{self.GLOBAL_BRANCH_NAME}/{key}"

    def get_global_group(self, key: str, force_create: bool = True) -> h5py.Group:
        """Fetches a Global Group by key. It pre-prend 'global' branch name to desired key
        to compose the fulle key, like: /<GLOBAL_BRANCH_NAME>/key

        :param key: sample key
        :type key: str
        :param force_create: True to create group if not present, defaults to True
        :type force_create: bool, optional
        :return: fetched Group if any
        :rtype: h5py.Group
        """

        group_key = self.get_global_group_name(key=key)
        return self.get_group(group_key, force_create=force_create)

    def sample_keys(self) -> set:
        group = self.get_sample_root(force_create=False)
        if group is not None:
            return set(group.keys())
        else:
            return set()


class H5ToolKit:
    ENCODING_STRING = "_encoding"
    ENCODING_BINARY = "pkl"
    OPTIONS = {"png": {"compress_level": 4}}
    ALLOWED_ENCODINGS = DataCoding.IMAGE_CODECS + (ENCODING_BINARY,)

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
            if encoding in cls.ALLOWED_ENCODINGS:
                data = DataCoding.bytes_to_data(dataset[...], encoding)
                if data is None:
                    data = pickle.loads(dataset[...])
            else:
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
                cls.set_encoding(group[key], cls.ENCODING_BINARY)

        elif DataCoding.is_image_extension(encoding):
            options = cls.OPTIONS.get(encoding, {})
            buffer = BytesIO(bytes())
            imageio.imwrite(buffer, data, format=encoding, **options)
            group[key] = buffer.getbuffer()
            group[key].attrs[cls.ENCODING_STRING] = encoding
        else:
            buffer = BytesIO(bytes())
            pickle.dump(data, buffer)
            group[key] = buffer.getbuffer()
            cls.set_encoding(group[key], cls.ENCODING_BINARY)
