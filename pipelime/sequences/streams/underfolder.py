from typing import Optional, Sequence, Tuple
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.streams.base import DatasetStream, ItemConverter
from pipelime.sequences.writers.filesystem import UnderfolderWriter
from pipelime.sequences.samples import Sample, SamplesSequence


class UnderfolderStream(DatasetStream):
    def __init__(self, folder: str, allowed_keys: Optional[Sequence[str]] = None) -> None:
        super().__init__()
        self._folder = folder
        self._dataset = UnderfolderReader(folder=folder)
        self._dataset.flush()
        self._allowed_keys = allowed_keys
        self._writer = None
        if len(self._dataset) > 0:
            self._writer = UnderfolderWriter(
                folder=folder,
                root_files_keys=self._dataset.get_reader_template().root_files_keys,
                extensions_map=self._dataset.get_reader_template().extensions_map,
            )

    def flush(self):
        return self._dataset.flush()

    def __len__(self):
        return len(self._dataset)

    def manifest(self) -> dict:
        """Returns the manifest of the dataset with infos about size and
        sample's keys.

        :raises ValueError: If the dataset is empty.
        :return: The manifest of the dataset.
        :rtype: dict
        """
        if len(self._dataset) > 0:
            sample = self._dataset[0]
            keys = list(sample.keys())
            return {
                "size": len(self),
                "keys": keys,
            }
        else:
            raise ValueError("Dataset is empty")

    def get_sample(self, sample_id: int) -> Sample:
        """Returns the sample with the given id.

        :param sample_id: The id of the sample.
        :type sample_id: int
        :raises ValueError: If the sample_id is out of range.
        :return: The sample with the given id.
        :rtype: Sample
        """
        if sample_id < len(self._dataset):
            return self._dataset[sample_id]
        else:
            raise ValueError(f"Sample id '{sample_id}' out of range")

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
        sample = self.get_sample(sample_id)
        if item in sample:
            return sample[item]
        else:
            raise ValueError(f"Item '{item}' not found")

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
        item = self.get_item(sample_id, item)
        mimetype = ItemConverter.format_to_mimetype(format)
        return ItemConverter.item_to_data(item, format), mimetype

    def set_data(
        self, sample_id: int, item: str, data: any, format: str
    ) -> Tuple[any, str]:
        """ Sets the sample's item with the given name in the given format.

        :param sample_id: The id of the sample.
        :type sample_id: int
        :param item: The name of the item.
        :type item: str
        :param data: The data to set [io.BytesIO, dict]
        :type data: any
        :param format: The format of the data.
        :type format: str
        :return: The item with the given name in the given format.
        :rtype: Tuple[any, str]
        """

        if self._allowed_keys is not None and item not in self._allowed_keys:
            raise ValueError(f"Item '{item}' not allowed")

        if self._writer is not None:
            sample = self.get_sample(sample_id)
            sample[item] = ItemConverter.data_to_item(data, format)
            self._writer(SamplesSequence([sample]))
