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


class SampleStagesFactory(object):

    FACTORY_MAP: Dict[str, SampleStage] = {}

    @classmethod
    def generic_stage_schema(cls) -> Schema:
        return Schema({
            'type': str,
            str: object
        })

    @classmethod
    def register_stage(cls, stage: SampleStage):
        cls.FACTORY_MAP[stage.__name__] = stage

    @classmethod
    def create(cls, cfg: dict) -> SampleStage:
        cls.generic_stage_schema().validate(cfg)
        _t = cls.FACTORY_MAP[cfg['type']]
        return _t.build_from_dict(cfg)


def register_stage_factory(s: SampleStage) -> SampleStage:
    """ Register a SampleStage to the factory

    :param s: stage to register
    :type s: SampleStage
    :return: the same stage. It is intent to be used as decorator
    :rtype: SampleStage
    """
    SampleStagesFactory.register_stage(s)
    return s


@register_stage_factory
class StageCompose(SampleStage):

    def __init__(self, stages: Sequence[SampleStage]):
        super().__init__()
        self._stages = stages

    def __call__(self, x: Sample) -> Sample:
        out = x
        for s in self._stages:
            out = s(out)
        return out

    @classmethod
    def stage_name(cls) -> str:
        return StageCompose.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.stage_name(),
            'stages': list
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        super().build_from_dict(d)
        stages = [SampleStagesFactory.create(s) for s in d['stages']]
        return StageCompose(stages=stages)

    def to_dict(self) -> dict:
        return {
            'type': self.stage_name(),
            'stages': [s.to_dict() for s in self._stages]
        }


@register_stage_factory
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


@register_stage_factory
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


@register_stage_factory
class StageKeysFilter(SampleStage):

    def __init__(self, keys: list, negate: bool = False):
        """ Filter sample keys

        :param keys: list of keys to preserve
        :type keys: list
        :param negate: TRUE to delete input keys, FALSE delete all but keys
        :type negate: bool
        """
        super().__init__()
        self._keys = keys
        self._negate = negate

    def __call__(self, x: Sample) -> Sample:

        out: Sample = x.copy()
        for k in x.keys():
            condition = (k in self._keys) if self._negate else (k not in self._keys)
            if condition:
                del out[k]
        return out

    @classmethod
    def stage_name(cls) -> str:
        return StageKeysFilter.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.stage_name(),
            'options': {
                'keys': list,
                'negate': bool
            }
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        super().build_from_dict(d)
        return StageKeysFilter(**d['options'])

    def to_dict(self) -> dict:
        return {
            'type': self.stage_name(),
            'options': {
                'keys': self._keys,
                'negate': self._negate
            }
        }


@register_stage_factory
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
                'targets': dict
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
