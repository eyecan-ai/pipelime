import imghdr
import json
import pickle
import warnings
from collections import defaultdict
from pathlib import Path
from typing import Any, Union, Iterable, Sequence, BinaryIO, TextIO, Tuple, List

import imageio
import numpy as np
import yaml
import toml
from io import BytesIO, TextIOWrapper

from loguru import logger

from pipelime.tools.bytes import DataCoding
import pipelime.filesystem.remotes as plr


class FSToolkit(object):

    # Default imageio options for each image format
    IMG_SAVE_OPTIONS = {"png": {"compress_level": 4}}

    YAML_EXT = ("yaml", "yml")
    JSON_EXT = ("json",)
    TOML_EXT = ("toml", "tml")
    METADATA_EXT = YAML_EXT + JSON_EXT + TOML_EXT

    NUMPY_TXT_EXT = ("txt", "data")
    NUMPY_NATIVE_EXT = ("npy", "npz")
    NUMPY_EXT = NUMPY_TXT_EXT + NUMPY_NATIVE_EXT

    PICKLE_EXT = ("pkl", "pickle")

    BINARY_EXT = ("bin",)

    REMOTE_EXT = ("plr", "rmt", "remote")

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
        return ext if with_dot else ext.lstrip(".")

    @classmethod
    def is_metadata_file(cls, filename: str) -> bool:
        ext = cls.get_file_extension(filename)
        return ext in cls.METADATA_EXT

    @classmethod
    def is_remote_file(cls, filename: str) -> bool:
        ext = cls.get_file_extension(filename)
        return ext in cls.REMOTE_EXT

    @classmethod
    def is_image_file(cls, filename: Union[str, BinaryIO]) -> bool:
        return imghdr.what(filename) is not None  # type: ignore

    @classmethod
    def _numpy_load_txt(cls, filename: Union[str, TextIO, BinaryIO]) -> np.ndarray:
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
    def is_binary_file(cls, filename: str):
        ext = cls.get_file_extension(filename)
        return ext in cls.BINARY_EXT

    @classmethod
    def _download_from_remote_list(
        cls, remote_list: Union[Sequence[str], TextIO]
    ) -> Tuple[str, BytesIO]:
        extension = ""
        data_stream = None
        for line in remote_list:  # download from first available remote
            line = line.strip()
            remote, rm_base, rm_name = plr.get_remote_and_paths(line)
            if remote and rm_base and rm_name:
                extension = cls.get_file_extension(rm_name)
                data_stream = BytesIO()
                if not remote.download_stream(data_stream, rm_base, rm_name):
                    data_stream = None
            if data_stream is None:
                logger.debug(f"unknown or unreachable remote: {line}")
            else:
                data_stream.seek(0)
                break
        else:  # no remote loaded
            raise Exception("remote loading error")
        return extension, data_stream

    @classmethod
    def _download_from_remote(cls, filename: str) -> Tuple[str, BytesIO]:
        with open(filename, "r") as fd:
            return cls._download_from_remote_list(fd)

    @classmethod
    def load_remote_list(cls, filename: str) -> List[str]:
        url_list = []
        with open(filename, "r") as fd:
            for line in fd:
                url_list.append(line.strip())
        return url_list

    @classmethod
    def is_remote_list(cls, data: Any) -> bool:
        from urllib.parse import urlparse

        if not isinstance(data, Sequence):
            return False
        for d in data:
            if not isinstance(d, str):
                return False
            try:
                parsed_url = urlparse(d)
                if not parsed_url.scheme:
                    return False
            except ValueError:
                return False
        return True

    @classmethod
    def load_data_from_stream(
        cls, data_stream: BinaryIO, extension: str
    ) -> Union[None, np.ndarray, dict, bytes]:
        switches = (
            (
                lambda: extension in cls.YAML_EXT,
                lambda: yaml.safe_load(TextIOWrapper(data_stream)),
            ),
            (
                lambda: extension in cls.JSON_EXT,
                lambda: json.load(TextIOWrapper(data_stream)),
            ),
            (
                lambda: extension in cls.TOML_EXT,
                lambda: dict(toml.load(TextIOWrapper(data_stream))),
            ),
            (
                lambda: extension in cls.PICKLE_EXT,
                lambda: pickle.load(data_stream),
            ),
            (
                lambda: extension in cls.BINARY_EXT,
                lambda: data_stream.read(),
            ),
            (
                lambda: extension in cls.NUMPY_TXT_EXT,
                lambda: np.atleast_2d(cls._numpy_load_txt(data_stream)),
            ),
            (
                lambda: extension in cls.NUMPY_NATIVE_EXT,
                lambda: np.atleast_2d(np.load(data_stream)),
            ),
            # image must be tested last, since it looks at the binary content
            (
                lambda: cls.is_image_file(data_stream),
                lambda: np.array(imageio.imread(data_stream)),
            ),
        )

        for cond, fn in switches:
            if cond():
                return fn()
        return None

    @classmethod
    def load_remote_data(
        cls, remote_list: Sequence[str]
    ) -> Union[None, np.ndarray, dict, bytes]:
        try:
            extension, data_stream = cls._download_from_remote_list(remote_list)
            data = cls.load_data_from_stream(data_stream, extension)
            if data is not None:
                return data
        except Exception as e:
            raise Exception(f"Loading data error: {e}") from e
        raise NotImplementedError(f"Unknown data extension: {extension}")

    @classmethod
    def load_data(cls, filename: str) -> Union[None, np.ndarray, dict, bytes]:
        """Load data from file based on its extension

        :param filename: target filename
        :type filename: str
        :return: Loaded data as array or dict. May return NONE
        :rtype: Union[None, np.ndarray, dict]
        """
        data_stream = None
        try:
            extension = cls.get_file_extension(filename)
            if extension in cls.REMOTE_EXT:
                extension, data_stream = cls._download_from_remote(filename)

            if data_stream is None:
                data_stream = open(filename, "rb")

            data = cls.load_data_from_stream(data_stream, extension)
            if data is not None:
                return data
        except Exception as e:
            raise Exception(f"Loading data error: {e}") from e
        finally:
            if data_stream is not None:
                data_stream.close()

        raise NotImplementedError(f"Unknown file extension: {filename}")

    @classmethod
    def store_data_to_stream(cls, data_stream: BinaryIO, extension: str, data: Any):
        try:

            def _write_remote_file(filestream, data):
                if isinstance(data, Iterable):
                    data = "\n".join(data)
                filestream.write(data)

            extension = extension.lstrip(".")

            switches = (
                (
                    lambda: DataCoding.is_image_extension(extension),
                    lambda: imageio.imwrite(
                        data_stream,
                        data,
                        format=f".{extension}",
                        **(cls.IMG_SAVE_OPTIONS.get(extension, {})),
                    ),
                ),
                (
                    lambda: DataCoding.is_text_extension(extension),
                    lambda: np.savetxt(data_stream, data),
                ),
                (
                    lambda: DataCoding.is_numpy_extension(extension),
                    lambda: np.save(data_stream, data),
                ),
                (
                    lambda: extension in cls.YAML_EXT,
                    lambda: yaml.safe_dump(data, TextIOWrapper(data_stream)),
                ),
                (
                    lambda: extension in cls.JSON_EXT,
                    lambda: json.dump(data, TextIOWrapper(data_stream)),
                ),
                (
                    lambda: extension in cls.TOML_EXT,
                    lambda: toml.dump(data, TextIOWrapper(data_stream)),
                ),
                (
                    lambda: DataCoding.is_pickle_extension(extension),
                    lambda: pickle.dump(data, data_stream),
                ),
                (
                    lambda: extension in cls.BINARY_EXT,
                    lambda: data_stream.write(data),
                ),
                (
                    lambda: extension in cls.REMOTE_EXT,
                    lambda: _write_remote_file(TextIOWrapper(data_stream), data),
                ),
            )

            for cond, fn in switches:
                if cond():
                    fn()
                    return
        except Exception as e:
            raise Exception(f"Loading data error: {e}") from e

        # extension not found in switches
        raise NotImplementedError(f"Unknown file extension: {extension}")

    @classmethod
    def store_data(cls, filename: str, data: Any):
        data_stream = None
        try:
            extension = cls.get_file_extension(filename)
            data_stream = open(filename, "wb")
        except Exception as e:
            raise Exception(f"Loading data error: {e}") from e
        else:
            return cls.store_data_to_stream(data_stream, extension, data)
        finally:
            if data_stream is not None:
                data_stream.close()

    @classmethod
    def start_file(cls, filename: str):
        import subprocess
        import os
        import platform

        if platform.system() == "Darwin":  # macOS
            subprocess.call(("open", filename))
        elif platform.system() == "Windows":  # Windows
            os.startfile(filename)
        else:  # linux variants #TODO: verify!
            subprocess.call(("xdg-open", filename))
