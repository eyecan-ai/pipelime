from abc import abstractmethod
from typing import Hashable, Sequence
from pipelime.sequences.api.entities import EntityDataset, EntitySample
import io


class SequenceInterface:
    @abstractmethod
    def list_datasets(self) -> Sequence[EntityDataset]:
        raise NotImplementedError()

    @abstractmethod
    def get_dataset(self, dataset_name: str) -> EntityDataset:
        raise NotImplementedError()

    @abstractmethod
    def get_sample(self, dataset_name: str, sample_id: Hashable) -> EntitySample:
        raise NotImplementedError()

    @abstractmethod
    def get_sample_data(
        self, sample_id: int, item_name: str, format: str = None
    ) -> io.BytesIO:
        raise NotImplementedError()

    @abstractmethod
    def put_sample(
        self, dataset_name: str, sample_id: Hashable, sample_entity: EntitySample
    ) -> EntitySample:
        raise NotImplementedError()

    @abstractmethod
    def search_samples(self, proto_sample: EntitySample) -> Sequence[EntitySample]:
        raise NotImplementedError()
