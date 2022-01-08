from abc import abstractmethod
from typing import Dict, Hashable, Optional, Sequence

from pydantic.main import BaseModel


class EntitySample(BaseModel):
    id: int
    metadata: dict
    data: Optional[dict]


class EntityDataset(BaseModel):
    name: str
    manifest: dict


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
    def put_sample(
        self, dataset_name: str, sample_id: Hashable, sample_entity: EntitySample
    ) -> EntitySample:
        raise NotImplementedError()
