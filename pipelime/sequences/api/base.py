from abc import abstractmethod
from typing import Dict, Hashable, Optional, Sequence

from pydantic.main import BaseModel
import rich


class EntitySampleData(BaseModel):
    type: str
    encoding: str


class EntitySample(BaseModel):
    id: int
    metadata: dict
    data: Optional[Dict[str, EntitySampleData]]


class EntityDatasetManifest(BaseModel):
    size: int
    keys: Sequence[str]
    sample_ids: Sequence[int]


class EntityDataset(BaseModel):
    name: str
    manifest: EntityDatasetManifest


class ParamPagination(BaseModel):
    paginationStart: int = 0
    paginationSize: int = 10
    paginationEnd: Optional[int] = None

    def filter(self, s: Sequence[any]) -> Sequence[any]:

        start = self.paginationStart
        end = (
            self.paginationEnd + 1
            if self.paginationEnd is not None
            else start + self.paginationSize
        )
        return s[start:end]


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