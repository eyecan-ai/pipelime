from typing import Dict, Hashable, KeysView, Optional, Sequence, Tuple
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.streams.base import DatasetStream, ItemConverter
from pipelime.sequences.writers.filesystem import UnderfolderWriter
from pipelime.sequences.samples import Sample, SamplesSequence


class UnderfolderStream(DatasetStream):
    def __init__(
        self, folder: str, allowed_keys: Optional[Sequence[str]] = None
    ) -> None:
        """Creates an UnderfolderStream object that reads and writes samples from and to
        the given Underfolder format dataset

        :param folder: the Undefolder folder
        :type folder: str
        :param allowed_keys: list of allowed keys the user can modify , defaults to None
        :type allowed_keys: Optional[Sequence[str]], optional
        """
        super().__init__()
        self._folder = folder
        self._reader = UnderfolderReader(folder=folder)
        self._reader.flush()
        self._allowed_keys = allowed_keys
        self._writer = None
        self._samples_map = {}
        for sample in self._reader:
            self._samples_map[sample.id] = sample

        if len(self._reader) > 0:
            self._reload_writer()

    def _reload_writer(
        self,
        additional_root_files_keys: Optional[Sequence[str]] = None,
        additional_extensions_map: Optional[Dict[str, str]] = None,
    ) -> None:

        root_files_keys = self._reader.get_reader_template().root_files_keys
        extensions_map = self._reader.get_reader_template().extensions_map

        if additional_root_files_keys is not None:
            root_files_keys = root_files_keys + additional_root_files_keys

        if additional_extensions_map is not None:
            extensions_map.update(additional_extensions_map)

        self._writer = UnderfolderWriter(
            folder=self._folder,
            root_files_keys=root_files_keys,
            extensions_map=extensions_map,
            zfill=self._reader.get_reader_template().idx_length,
            remove_duplicates=True,
        )

    def add_root_files_keys(self, root_files_keys: Sequence[str]) -> None:
        """Adds the root files keys of the dataset.

        :param root_files_keys: The root files keys of the dataset.
        :type root_files_keys: Sequence[str]
        """
        self._reload_writer(additional_root_files_keys=root_files_keys)

    def add_extensions_map(self, extensions_map: Dict[str, str]) -> None:
        """Adds the extensions map of the dataset.

        :param extensions_map: The extensions map of the dataset.
        :type extensions_map: Dict[str, str]
        """
        self._reload_writer(additional_extensions_map=extensions_map)

    def flush(self):
        return self._reader.flush()

    def __len__(self):
        return len(self._reader)

    def manifest(self) -> dict:
        """Returns the manifest of the dataset with infos about size and
        sample's keys.

        :raises ValueError: If the dataset is empty.
        :return: The manifest of the dataset.
        :rtype: dict
        """
        if len(self._reader) > 0:
            sample = self._reader[0]
            keys = list(sample.keys())
            return {
                "size": len(self),
                "keys": keys,
            }
        else:
            raise ValueError("Dataset is empty")

    def get_sample_ids(self) -> KeysView:
        """Get all the sample ids of the dataset.

        :return: The sample ids of the dataset.
        :rtype: KeysView
        """
        return self._samples_map.keys()

    def get_sample(self, sample_id: Hashable) -> Sample:
        """Returns the sample with the given id.

        :param sample_id: The id of the sample.
        :type sample_id: Hashable
        :raises KeyError: If the sample_id is out of range.
        :return: The sample with the given id.
        :rtype: Sample
        """
        if sample_id in self._samples_map:
            return self._samples_map[sample_id]
        else:
            raise KeyError(f"Sample id '{sample_id}' not found")

    def get_item(self, sample_id: Hashable, item: str) -> any:
        """Returns the sample's item with the given name.

        :param sample_id: The id of the sample.
        :type sample_id: Hashable
        :param item: The name of the item.
        :type item: str
        :raises KeyError: If the sample_id is out of range or if the item is not in the sample.
        :return: The item with the given name.
        :rtype: any
        """
        sample = self.get_sample(sample_id)
        if item in sample:
            return sample[item]
        else:
            raise KeyError(f"Item '{item}' not found")

    def get_data(self, sample_id: Hashable, item: str, format: str) -> Tuple[any, str]:
        """Returns the sample's item with the given name in the given format.

        :param sample_id: The id of the sample.
        :type sample_id: Hashable
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
        self, sample_id: Hashable, item: str, data: any, format: str
    ) -> Tuple[any, str]:
        """Sets the sample's item with the given name in the given format.

        :param sample_id: The id of the sample.
        :type sample_id: Hashable
        :param item: The name of the item.
        :type item: str
        :param data: The data to set [io.BytesIO, dict]
        :type data: any
        :param format: The format of the data.
        :type format: str
        :raises KeyError: If the sample_id is not found
        :raises PermissionError: If the item key is not allowed to be modified.
        :return: The item with the given name in the given format.
        :rtype: Tuple[any, str]
        """
        if sample_id not in self._samples_map:
            raise KeyError(f"Sample id '{sample_id}' not found")

        if self._allowed_keys is not None and item not in self._allowed_keys:
            raise PermissionError(f"Item '{item}' not allowed")

        if self._writer is not None:
            sample = self.get_sample(sample_id)
            old_keys = list(sample.keys())
            sample[item] = ItemConverter.data_to_item(data, format)

            # Copy sample in order to delete key only on current instance
            sample = sample.copy()
            for key in old_keys:
                if key != item:
                    del sample[key]
            self._writer(SamplesSequence([sample]))
