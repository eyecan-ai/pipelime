from pydantic.main import BaseModel
from typing import Dict, Optional, Sequence


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


class EntityPagination(BaseModel):
    offset: Optional[int] = 0
    limit: Optional[int] = 50
    total_count: Optional[int] = None

    @classmethod
    def create_from_sequence(self, Sequence: Sequence[any]) -> None:
        pagination = EntityPagination()
        pagination.offset = 0
        pagination.limit = len(Sequence)
        pagination.total_count = pagination.limit
        return pagination

    def filter(self, s: Sequence[any]) -> Sequence[any]:
        return s[self.offset : self.offset + self.limit]


class EntitySampleSearchRequest(BaseModel):
    proto_sample: Optional[EntitySample]
    only_pagination: Optional[bool] = False
    pagination: Optional[EntityPagination] = None


class EntitySampleSearchResponse(BaseModel):
    samples: Sequence[EntitySample] = []
    pagination: EntityPagination
