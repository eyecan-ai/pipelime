from abc import abstractmethod
from typing import Sequence, Tuple
from pathlib import Path

import imageio
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.writers.filesystem import UnderfolderWriter
from pipelime.sequences.samples import Sample, SamplesSequence
import numpy as np
import cv2
import io


class ItemConverter:
    IMAGE_FORMATS = ["jpg", "jpeg", "png"]
    MATRIX_FORMATS = ["matrix"]
    DICT_FORMATS = ["dict"]

    @classmethod
    def data_to_item(cls, data: any, format: str) -> any:
        """Convert data to item.

        :param data: data to convert
        :type data: any
        :param format: format of data
        :type format: str
        :raises NotImplementedError: if format is not supported
        :raises ValueError: if data is not supported
        :return: converted data
        :rtype: any
        """
        if format in cls.IMAGE_FORMATS:
            return imageio.imread(data.getbuffer())
        elif format in cls.MATRIX_FORMATS:
            return np.array(data["data"])
        elif format in cls.DICT_FORMATS:
            return data
        else:
            raise ValueError(f"Unknown format {format}")

    @classmethod
    def item_to_image_data(cls, item: np.ndarray, format: str) -> io.BytesIO:
        """Converts a numpy array to a BytesIO object.

        :param item: The numpy array to convert.
        :type item: np.ndarray
        :param format: The format of the image.
        :type format: str
        :return: The BytesIO object.
        :rtype: io.BytesIO
        """
        item = cv2.cvtColor(item, cv2.COLOR_BGR2RGB)
        _, im_png = cv2.imencode(f".{format}", item)
        return io.BytesIO(im_png.tobytes())

    @classmethod
    def item_to_matrix_data(cls, item: np.ndarray, format: str) -> dict:
        """Converts a numpy array to a dictionary {data:[...]}

        :param item: The numpy array to convert.
        :type item: np.ndarray
        :param format: The format of the numeric data.
        :type format: str
        :return: The dictionary {data:[...]}
        :rtype: dict
        """
        return {"data": item.tolist()}

    @classmethod
    def item_to_dict_data(cls, item: dict, format: str) -> dict:
        """Converts a dictionary to a dictionary ?? COPILOT fault :)

        :param item: The dictionary to convert.
        :type item: dict
        :param format: The format of the specific dictionary.
        :type format: str
        :return: the output dictionary
        :rtype: dict
        """
        return item

    @classmethod
    def item_to_data(cls, item: any, format: str) -> any:
        """Converts an item to a specific format.

        :param item: The item to convert.
        :type item: any
        :param format: The format of the specific item.
        :type format: str
        :raises ValueError: If the format is not supported.
        :return: The converted item.
        :rtype: any
        """
        if format in cls.IMAGE_FORMATS:
            return cls.item_to_image_data(item, format)
        elif format in cls.MATRIX_FORMATS:
            return cls.item_to_matrix_data(item, format)
        elif format in cls.DICT_FORMATS:
            return cls.item_to_dict_data(item, format)
        else:
            raise ValueError(f"Format {format} not supported yet")

    @classmethod
    def format_to_mimetype(cls, format: str) -> str:
        """Converts a format string to the corresponding mimetype.

        :param format: The format to convert.
        :type format: str
        :raises ValueError: If the format is not supported.
        :return: The mimetype.
        :rtype: str
        """
        if format in cls.IMAGE_FORMATS:
            return f"image/{format}"
        elif format in cls.MATRIX_FORMATS:
            return f"application/json"
        elif format in cls.DICT_FORMATS:
            return f"application/json"
        else:
            raise ValueError(f"Format {format} not supported yet")


class DatasetStream:
    def __init__(self) -> None:
        pass

    @abstractmethod
    def __len__(self):
        pass

    def flush(self):
        pass

    @abstractmethod
    def manifest(self) -> dict:
        """Returns the manifest of the dataset with infos about size and
        sample's keys.

        :raises ValueError: If the dataset is empty.
        :return: The manifest of the dataset.
        :rtype: dict
        """
        pass

    @abstractmethod
    def get_sample(self, sample_id: int) -> Sample:
        """Returns the sample with the given id.

        :param sample_id: The id of the sample.
        :type sample_id: int
        :raises ValueError: If the sample_id is out of range.
        :return: The sample with the given id.
        :rtype: Sample
        """
        pass

    @abstractmethod
    def get_item(self, sample_id: int, item: str) -> any:
        """Returns the sample's item with the given name.

        :param sample_id: The id of the sample.
        :type sample_id: int
        :param item: The name of the item.
        :type item: str
        :raises ValueError: If the sample_id is out of range or if the item is not in the sample.
        :return: The item with the given name.
        :rtype: any
        """

    @abstractmethod
    def get_data(self, sample_id: int, item: str, format: str) -> Tuple[any, str]:
        """Returns the sample's item with the given name in the given format.

        :param sample_id: The id of the sample.
        :type sample_id: int
        :param item: The name of the item.
        :type item: str
        :param format: The format of the item.
        :type format: str
        :return: The item with the given name in the given format.
        :rtype: Tuple[any, str]
        """
        pass

    @abstractmethod
    def set_data(
        self, sample_id: int, item: str, data: any, format: str
    ) -> Tuple[any, str]:
        """ Sets the sample's item with the given name in the given format.

        :param sample_id: The id of the sample.
        :type sample_id: int
        :param item: The name of the item.
        :type item: str
        :param data: The data to set.
        :type data: any
        :param format: The format of the data.
        :type format: str
        :return: The item with the given name in the given format.
        :rtype: Tuple[any, str]
        """
        pass
