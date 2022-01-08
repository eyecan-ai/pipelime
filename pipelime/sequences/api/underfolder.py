from typing import Dict, Hashable, Sequence
from pipelime.sequences.api.base import (
    EntityDataset,
    EntitySample,
    SequenceInterface,
)
from pipelime.sequences.samples import FileSystemSample
from pipelime.sequences.streams.underfolder import UnderfolderStream
from pipelime.filesystem.toolkit import FSToolkit
from pipelime.tools.bytes import DataCoding


class UnderfolderInterface(SequenceInterface):
    def __init__(self, name: str, folder: str) -> None:
        self._name = name
        self._stream = UnderfolderStream(folder=folder)

    def _get_sample_entity(self, sample_id: int):

        # create entity
        entity = EntitySample(id=sample_id, metadata={}, data={})

        # get raw sample
        raw_sample: FileSystemSample = self._stream.get_sample(sample_id)

        # fill metadata and urls
        for key in raw_sample:
            filename = raw_sample.filesmap[key]
            extension = FSToolkit.get_file_extension(filename)
            if DataCoding.is_metadata_extension(extension):
                entity.metadata[key] = raw_sample[key]
            else:
                # TODO: move this kind of "File Description" in unique proxy
                if DataCoding.is_image_extension(extension):
                    entity.data[key] = {"type": "image", "encoding": extension}
                elif DataCoding.is_numpy_extension(extension):
                    entity.data[key] = {"type": "numpy", "encoding": extension}
                elif DataCoding.is_pickle_extensio(extension):
                    entity.data[key] = {"type": "pickle", "encoding": extension}
                elif DataCoding.is_text_extension(extension):
                    entity.data[key] = {"type": "text", "encoding": extension}

        return entity

    def _put_sample_entity(self, sample_id: int, entity: EntitySample):
        for key in entity.metadata:
            self._stream.set_data(sample_id, key, entity.metadata[key], format="dict")

    def get_dataset(self) -> EntityDataset:
        return EntityDataset(name=self._name, manifest=self._stream.manifest())

    def get_sample(self, sample_id: int) -> EntitySample:
        if sample_id in self._stream.get_sample_ids():
            return self._get_sample_entity(sample_id)
        else:
            raise KeyError(f"Sample {sample_id} not found")

    def get_sample_data(
        self, sample_id: int, item_name: str, format: str = None
    ) -> any:
        if sample_id in self._stream.get_sample_ids():
            entity = self._get_sample_entity(sample_id)
            if item_name in entity.data:
                item = entity.data[item_name]
                data_format = format if format is not None else item["encoding"]
                return self._stream.get_data(sample_id, item_name, format=data_format)
            else:
                raise KeyError(f"Item {item_name} not found")
        else:
            raise KeyError(f"Sample {sample_id} not found")

    def put_sample(self, sample_id: int, sample_entity: EntitySample) -> EntitySample:
        if sample_id in self._stream.get_sample_ids():
            self._put_sample_entity(sample_id, sample_entity)
            return self._get_sample_entity(sample_id)
        else:
            raise KeyError(f"Sample {sample_id} not found")
