from io import BytesIO
from pathlib import Path
from typing import Tuple

import imageio
import numpy as np


class DataCoding(object):

    IMAGE_CODECS = ("jpg", "jpeg", "png", "tiff", "bmp")
    IMAGE_CODECS_IS_LOSSY = {"jpg": True, "jpeg": True}
    IMAGE_CODECS_HAS_ALPHA = {"png": True}

    NUMPY_CODECS = ("npy",)
    TEXT_CODECS = ("txt",)
    METADATA_CODECS = ("json", "yml", "yaml", "toml", "tml")
    PICKLE_CODECS = ("pkl", "pickle")

    @classmethod
    def is_image_extension(cls, extension: str):
        return extension in DataCoding.IMAGE_CODECS

    @classmethod
    def is_text_extension(cls, extension: str):
        return extension in DataCoding.TEXT_CODECS

    @classmethod
    def is_numpy_extension(cls, extension: str):
        return extension in DataCoding.NUMPY_CODECS

    @classmethod
    def is_metadata_extension(cls, extension: str):
        return extension in DataCoding.METADATA_CODECS

    @classmethod
    def is_pickle_extension(cls, extension: str):
        return extension in DataCoding.PICKLE_CODECS

    @classmethod
    def bytes_to_data(cls, data: bytes, data_encoding: str) -> np.ndarray:
        """Converts bytes with corresponding encoding into numpy array

        :param data: source bytes
        :type data: bytes
        :param data_encoding: bytes encoding
        :type data_encoding: str
        :return: numpy array
        :rtype: np.ndarray
        """
        data_encoding = data_encoding.replace(".", "")

        if data_encoding in cls.IMAGE_CODECS:
            buffer = BytesIO(data)
            return imageio.imread(buffer.getbuffer(), format=data_encoding)
        elif data_encoding in cls.NUMPY_CODECS:
            buffer = BytesIO(data)
            return np.load(buffer)
        elif data_encoding in cls.TEXT_CODECS:
            buffer = BytesIO(data)
            return np.loadtxt(buffer)
        else:
            return None

    @classmethod
    def file_to_bytes(cls, filename: str) -> Tuple[bytes, str]:
        """Converts image from file as (bytes, encoding)

        :param filename: source filename
        :type filename: str
        :return: pair (bytes, codec string)
        :rtype: Tuple[bytes, str]
        """
        filename = Path(filename)
        extension = filename.suffix.replace(".", "")
        return open(filename, "rb").read(), extension

    @classmethod
    def numpy_image_to_bytes(cls, array: np.ndarray, data_encoding: str) -> bytes:
        """Converts image stored as numpy array into bytes with custom data encoding

        :param array: source image numpy array
        :type array: np.ndarray
        :param data_encoding: codec string representation
        :type data_encoding: str
        :return: bytes representation
        :rtype: bytes
        """
        data_encoding = data_encoding.replace(".", "")

        data = bytes()
        if data_encoding in cls.IMAGE_CODECS:
            buffer = BytesIO(data)
            imageio.imwrite(buffer, array, format=data_encoding)
            return buffer.getvalue()
        else:
            return None

    @classmethod
    def numpy_image_to_bytes_buffer(
        cls, array: np.ndarray, data_encoding: str
    ) -> bytes:
        """Converts image stored as numpy array into bytes buffer with custom data encoding

        :param array: source image numpy array
        :type array: np.ndarray
        :param data_encoding: codec string representation
        :type data_encoding: str
        :return: bytes representation
        :rtype: bytes
        """
        data_encoding = data_encoding.replace(".", "")

        data = bytes()
        if data_encoding in cls.IMAGE_CODECS:
            buffer = BytesIO(data)
            imageio.imwrite(buffer, array, format=data_encoding)
            return buffer.getbuffer()
        else:
            return None

    @classmethod
    def numpy_array_to_bytes(
        cls, array: np.ndarray, data_encoding: str = "npy"
    ) -> bytes:
        """Converts array data stored as numpy array into bytes with custom data encoding

        :param array: source generic numpy array
        :type array: np.ndarray
        :param data_encoding: codec string representation
        :type data_encoding: str
        :return: bytes representation
        :rtype: bytes
        """
        data_encoding = data_encoding.replace(".", "")

        data = bytes()
        if data_encoding in cls.NUMPY_CODECS:
            buffer = BytesIO(data)
            np.save(buffer, array, allow_pickle=True)
            return buffer.getvalue()
        else:
            return None

    @classmethod
    def is_codec_lossy(cls, codec: str) -> bool:
        """Checks if codec should be lossy

        :param codec: [description]
        :type codec: str
        :return: TRUE if codec is lossy
        :rtype: bool
        """

        if codec in cls.IMAGE_CODECS_IS_LOSSY:
            return cls.IMAGE_CODECS_IS_LOSSY[codec]
        return False

    @classmethod
    def has_codec_alpha(cls, codec: str) -> bool:
        """Checks if codec has alpha channel

        :param codec: [description]
        :type codec: str
        :return: TRUE for alpha channel available
        :rtype: bool
        """

        if codec in cls.IMAGE_CODECS_HAS_ALPHA:
            return cls.IMAGE_CODECS_HAS_ALPHA[codec]
        return False
