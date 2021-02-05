from abc import ABC, abstractmethod
from typing import Dict, Hashable, Sequence, Union

from schema import Schema
from pipelime.sequences.samples import Sample


class SampleStage(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def __call__(self, x:
                 Union[
                     Sample,
                     Sequence[Sample],
                     Dict[Hashable, Sample]
                 ]) -> Union[
                     Sample,
                     Sequence[Sample],
                     Dict[Hashable, Sample]
    ]:
        pass

    @classmethod
    @abstractmethod
    def stage_name(cls) -> str:
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def factory_schema(cls) -> Schema:
        raise NotImplementedError()

    @classmethod
    def build_from_dict(cls, d: dict):
        cls.factory_schema().validate(d)

    @abstractmethod
    def to_dict(self) -> dict:
        pass


class StageIdentity(SampleStage):

    def __init__(self):
        super().__init__()

    def __call__(self, x: any) -> any:
        return x

    @classmethod
    def stage_name(cls) -> str:
        return StageIdentity.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.stage_name(),
            'options': {}
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        super().build_from_dict(d)
        return StageIdentity()

    def to_dict(self) -> dict:
        return {
            'type': self.stage_name(),
            'options': {}
        }


class StageRemap(SampleStage):

    def __init__(self, remap: dict, remove_missing: bool = True):
        super().__init__()
        self._remap = remap
        self._remove_missing = remove_missing

    def __call__(self, x: Sample) -> Sample:

        out: Sample = x.copy()
        for k in x.keys():
            # for k, v in x.items():
            if k in self._remap:
                out.rename(k, self._remap[k])
            else:
                if self._remove_missing:
                    del out[k]
        return out

    @classmethod
    def stage_name(cls) -> str:
        return StageRemap.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.stage_name(),
            'options': {
                'remap': dict,
                'remove_missing': bool
            }
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        super().build_from_dict(d)
        return StageRemap(
            remap=d['options']['remap'],
            remove_missing=d['options']['remove_missing']
        )

    def to_dict(self) -> dict:
        return {
            'type': self.stage_name(),
            'options': {
                'remap': self._remap,
                'remove_missing': self._remove_missing
            }
        }
