import imghdr
import json
import pickle
import warnings
from collections import defaultdict
from pathlib import Path
from typing import Any, Union

import imageio
import numpy as np
import yaml
import toml

from pipelime.tools.bytes import DataCoding


class FSToolkit(object):

    # Default imageio options for each image format
    OPTIONS = {"png": {"compress_level": 4}}

    YAML_EXT = ("yaml", "yml")
    JSON_EXT = ("json",)
    TOML_EXT = ("toml", "tml")
    METADATA_EXT = YAML_EXT + JSON_EXT + TOML_EXT

    NUMPY_TXT_EXT = ("txt", "data")
    NUMPY_NATIVE_EXT = ("npy", "npz")
    NUMPY_EXT = NUMPY_TXT_EXT + NUMPY_NATIVE_EXT

    PICKLE_EXT = ("pkl", "pickle")

    REMOTE_EXT = ("remote",)

    # Declare TREE structure
    @classmethod
    def tree(cls):
        return defaultdict(cls.tree)

    @classmethod
    def tree_from_underscore_notation_files(cls, folder: Union[str, Path]):
        """Walk through files in folder generating a tree based on Underscore notation.
        Leafs of abovementioned tree are string representing filenames, inner nodes
        represent keys hierarchy.

        :param folder: target folder
        :type folder: str
        :param skip_hidden_files: TRUE to skip files starting with '.', defaults to True
        :type skip_hidden_files: bool, optional
        :return: dictionary representing multilevel tree
        :rtype: dict
        """

        keys_tree = cls.tree()
        folder = Path(folder)
        files = list(sorted(folder.glob("*")))
        for f in files:
            f: Path

            if f.is_dir():
                continue

            name = f.stem

            if name.startswith("."):
                continue

            chunks = name.split("_", maxsplit=1)
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
        return ext in cls.METADATA_EXT

    @classmethod
    def is_image_file(cls, filename: str) -> bool:
        return imghdr.what(filename) is not None

    @classmethod
    def _numpy_load_txt(cls, filename: str) -> np.ndarray:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", "loadtxt")
            data = np.loadtxt(filename)
        return data

    @classmethod
    def is_numpy_array_file(cls, filename: str) -> bool:
        ext = cls.get_file_extension(filename)
        if ext in cls.NUMPY_TXT_EXT:
            try:
                cls._numpy_load_txt(filename)
                return True
            except Exception:
                return False
        elif ext in cls.NUMPY_NATIVE_EXT:
            try:
                np.load(filename)
                return True
            except Exception:
                return False
        return False

    @classmethod
    def is_pickle_file(cls, filename: str):
        ext = cls.get_file_extension(filename)
        return ext in cls.PICKLE_EXT

    @classmethod
    def load_data(cls, filename: str) -> Union[None, np.ndarray, dict]:
        """Load data from file based on its extension

        :param filename: target filename
        :type filename: str
        :return: Loaded data as array or dict. May return NONE
        :rtype: Union[None, np.ndarray, dict]
        """
        try:
            if cls.is_image_file(filename):
                return np.array(imageio.imread(filename))
            else:
                extension = cls.get_file_extension(filename)
                if extension in cls.YAML_EXT:
                    return yaml.safe_load(open(filename, "r"))
                elif extension in cls.JSON_EXT:
                    return json.load(open(filename))
                elif extension in cls.TOML_EXT:
                    return dict(toml.load(filename))
                elif extension in cls.PICKLE_EXT:
                    return pickle.load(open(filename, "rb"))
                elif extension in cls.NUMPY_EXT:
                    npdata = (
                        cls._numpy_load_txt(filename)
                        if extension in cls.NUMPY_TXT_EXT
                        else (
                            np.load(filename)
                            if extension in cls.NUMPY_NATIVE_EXT
                            else None
                        )
                    )
                    if npdata is not None:
                        return np.atleast_2d(npdata)
        except Exception as e:
            raise Exception(f"Loading data error: {e}")

        raise NotImplementedError(f"Unknown file extension: {filename}")

    @classmethod
    def store_data(cls, filename: str, data: Any):
        try:
            extension = cls.get_file_extension(filename)
            if DataCoding.is_image_extension(extension):
                options = cls.OPTIONS.get(extension, {})
                imageio.imwrite(filename, data, **options)
            elif DataCoding.is_text_extension(extension):
                np.savetxt(filename, data)
            elif DataCoding.is_numpy_extension(extension):
                np.save(filename, data)
            elif extension in cls.YAML_EXT:
                yaml.safe_dump(data, open(filename, "w"))
            elif extension in cls.JSON_EXT:
                json.dump(data, open(filename, "w"))
            elif extension in cls.TOML_EXT:
                toml.dump(data, open(filename, "w"))
            elif DataCoding.is_pickle_extension(extension):
                pickle.dump(data, open(filename, "wb"))
            else:
                raise NotImplementedError(f"Unknown file extension: {filename}")
        except Exception as e:
            raise Exception(f"Loading data error: {e}")
