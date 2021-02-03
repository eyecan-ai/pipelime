from pathlib import Path
import imghdr
from typing import Union
import numpy as np
import imageio
import yaml
import json


class FSToolkit(object):

    # @classmethod
    # def tree_from_underscore_notation_files(cls, folder, skip_hidden_files=True):
    #     """Walk through files in folder generating a tree based on Underscore notation.
    #     Leafs of abovementioned tree are string representing filenames, inner nodes represent
    #     keys hierarchy.

    #     :param folder: target folder
    #     :type folder: str
    #     :param skip_hidden_files: TRUE to skip files starting with '.', defaults to True
    #     :type skip_hidden_files: bool, optional
    #     :return: dictionary representing multilevel tree
    #     :rtype: dict
    #     """

    #     # Declare TREE structure
    #     def tree():
    #         return defaultdict(tree)

    #     keys_tree = tree()
    #     folder = Path(folder)
    #     files = list(sorted(folder.glob('*')))
    #     for f in files:
    #         name = f.stem

    #         if skip_hidden_files:
    #             if name.startswith('.'):
    #                 continue

    #         chunks = name.split('_', maxsplit=1)
    #         if len(chunks) == 1:
    #             chunks.append('none')
    #         p = keys_tree
    #         for index, chunk in enumerate(chunks):
    #             if index < len(chunks) - 1:
    #                 p = p[chunk]
    #             else:
    #                 p[chunk] = str(f)

    #     return dict(keys_tree)

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
    def is_file_image(cls, filename: str) -> bool:
        return imghdr.what(filename) is not None

    @classmethod
    def is_file_numpy_array(cls, filename: str) -> bool:
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
    def load_data(cls, filename: str) -> Union[None, np.ndarray, dict]:
        """ Load data from file based on its extension

        :param filename: target filename
        :type filename: str
        :return: Loaded data as array or dict. May return NONE
        :rtype: Union[None, np.ndarray, dict]
        """

        extension = cls.get_file_extension(filename)
        data = None

        if cls.is_file_image(filename):
            data = np.array(imageio.imread(filename))

        if cls.is_file_numpy_array(filename):
            if extension in ['txt']:
                data = np.loadtxt(filename)
            elif extension in ['npy', 'npz']:
                data = np.load(filename)
            if data is not None:
                data = np.atleast_2d(data)

        if cls.is_metadata_file(filename):
            if extension in ['yml']:
                data = yaml.safe_load(open(filename, 'r'))
            elif extension in ['json']:
                data = json.load(open(filename))

        return data
