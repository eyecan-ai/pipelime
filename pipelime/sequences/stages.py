import albumentations as A
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
        """ Remaps keys in sample

        :param remap: old_key:new_key dictionary remap
        :type remap: dict
        :param remove_missing: if TRUE missing keys in remap will be removed in the output sample, defaults to True
        :type remove_missing: bool, optional
        """
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


class StageAugmentations(SampleStage):

    def __init__(self, transform_cfg: dict, targets: dict):
        super().__init__()
        self._transform_cfg = transform_cfg
        self._targets = targets
        self._transform: A.Compose = A.from_dict(transform_cfg)
        self._transform.add_targets(self._purge_targets(self._targets))

    def _purge_targets(self, targets: dict):
        # TODO: could it be wrong if targets also contains 'image' or 'mask' (aka default target)?
        return targets

    def __call__(self, x: Sample) -> Sample:

        try:
            out = x.copy()
            to_transform = {}
            for k, data in x.items():
                if k in self._targets:
                    to_transform[k] = data

            _transformed = self._transform(**to_transform)
            for k in self._targets.keys():
                out[k] = _transformed[k]

            return out
        except Exception as e:
            raise Exception(f'Stage[{self.__class__.__name__}] -> {e}')

    @classmethod
    def stage_name(cls) -> str:
        return StageAugmentations.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.stage_name(),
            'options': {
                'transform_cfg': dict,
                'targets': bool
            }
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        super().build_from_dict(d)
        return StageAugmentations(
            transform_cfg=d['options']['transform_cfg'],
            targets=d['options']['targets']
        )

    def to_dict(self) -> dict:
        return {
            'type': self.stage_name(),
            'options': {
                'transform_cfg': self._transform_cfg,
                'targets': self._targets
            }
        }
