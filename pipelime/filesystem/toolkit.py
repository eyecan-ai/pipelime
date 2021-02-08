from pathlib import Path
import imghdr
import pickle
from pipelime.tools.bytes import DataCoding
from typing import Union
import numpy as np
import imageio
import yaml
import json
from collections import defaultdict


class FSToolkit(object):

    # Declare TREE structure
    @classmethod
    def tree(cls):
        return defaultdict(cls.tree)

    @classmethod
    def tree_from_underscore_notation_files(cls, folder: str):
        """Walk through files in folder generating a tree based on Underscore notation.
        Leafs of abovementioned tree are string representing filenames, inner nodes represent
        keys hierarchy.

        :param folder: target folder
        :type folder: str
        :param skip_hidden_files: TRUE to skip files starting with '.', defaults to True
        :type skip_hidden_files: bool, optional
        :return: dictionary representing multilevel tree
        :rtype: dict
        """

        keys_tree = cls.tree()
        folder = Path(folder)
        files = list(sorted(folder.glob('*')))
        for f in files:
            f: Path

            if f.is_dir():
                continue

            name = f.stem

            if name.startswith('.'):
                continue

            chunks = name.split('_', maxsplit=1)
            if len(chunks) == 1:
                continue

            p = keys_tree
            for index, chunk in enumerate(chunks):
                if index < len(chunks) - 1:
                    p = p[chunk]
                else:
                    p[chunk] = str(f)

        return dict(keys_tree)

    @classmethod
    def get_file_extension(cls, filename, with_dot=False):
        ext = Path(filename).suffix.lower()
        if not with_dot and len(ext) > 0:
            ext = ext[1:]
        return ext

    @classmethod
    def is_metadata_file(cls, filename: str) -> bool:
        ext = cls.get_file_extension(filename)
        return ext in ['yml', 'json', 'toml', 'tml']

    @classmethod
    def is_image_file(cls, filename: str) -> bool:
        return imghdr.what(filename) is not None

    @classmethod
    def is_numpy_array_file(cls, filename: str) -> bool:
        ext = cls.get_file_extension(filename)
        if ext in ['txt', 'data']:
            try:
                np.loadtxt(filename)
                return True
            except Exception:
                return False
        if ext in ['npy', 'npz']:
            try:
                np.load(filename)
                return True
            except Exception:
                return False
        return False

    @classmethod
    def is_picke_file(cls, filename: str):
        ext = cls.get_file_extension(filename)
        return ext == 'pkl'

    @classmethod
    def load_data(cls, filename: str) -> Union[None, np.ndarray, dict]:
        """ Load data from file based on its extension

        :param filename: target filename
        :type filename: str
        :return: Loaded data as array or dict. May return NONE
        :rtype: Union[None, np.ndarray, dict]
        """

        extension = cls.get_file_extension(filename)
        data = None

        if cls.is_image_file(filename):
            data = np.array(imageio.imread(filename))

        elif cls.is_numpy_array_file(filename):
            if extension in ['txt']:
                data = np.loadtxt(filename)
            elif extension in ['npy', 'npz']:
                data = np.load(filename)
            if data is not None:
                data = np.atleast_2d(data)

        elif cls.is_metadata_file(filename):
            if extension in ['yml']:
                data = yaml.safe_load(open(filename, 'r'))
            elif extension in ['json']:
                data = json.load(open(filename))
        elif cls.is_picke_file(filename):
            data = pickle.load(open(filename, 'rb'))
        else:
            raise NotImplementedError(f'Unknown file extension: {filename}')
        return data

    @classmethod
    def store_data(cls, filename: str, data: any):

        extension = cls.get_file_extension(filename)

        if DataCoding.is_image_extension(extension):
            imageio.imwrite(filename, data)
        elif DataCoding.is_text_extension(extension):
            np.savetxt(filename, data)
        elif DataCoding.is_numpy_extension(extension):
            np.save(filename, data)
        elif DataCoding.is_metadata_extension(extension):
            if extension in ['yml']:
                yaml.safe_dump(data, open(filename, 'w'))
            elif extension in ['json']:
                json.dump(data, open(filename, 'w'))
        elif DataCoding.is_pickle_extension(extension):
            pickle.dump(data, open(filename, 'wb'))
        else:
            raise NotImplementedError(f'Unknown file extension: {filename}')
