from abc import ABC, abstractmethod
from typing import Dict, Optional, Sequence

import albumentations as A
from choixe.spooks import Spook

from pipelime.sequences.samples import Sample


class SampleStage(ABC, Spook):
    def __init__(self):
        pass

    @abstractmethod
    def __call__(self, x: Sample) -> Sample:
        pass


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
    def spook_schema(cls) -> dict:
        return {"stages": list}

    @classmethod
    def from_dict(cls, d: dict):
        stages = [Spook.create(s) for s in d["stages"]]
        return StageCompose(stages=stages)

    def to_dict(self):
        return {"stages": [s.serialize() for s in self._stages]}


class StageIdentity(SampleStage):
    def __init__(self):
        super().__init__()

    def __call__(self, x: any) -> any:
        return x


class StageRemap(SampleStage):
    def __init__(self, remap: dict, remove_missing: bool = True):
        """Remaps keys in sample

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
            if k in self._remap:
                out.rename(k, self._remap[k])
            else:
                if self._remove_missing:
                    del out[k]
        return out

    @classmethod
    def spook_schema(cls) -> dict:
        return {"remap": dict, "remove_missing": bool}

    @classmethod
    def from_dict(cls, d: dict):
        return StageRemap(remap=d["remap"], remove_missing=d["remove_missing"])

    def to_dict(self):
        return {"remap": self._remap, "remove_missing": self._remove_missing}


class StageKeysFilter(SampleStage):
    def __init__(self, keys: list, negate: bool = False):
        """Filter sample keys

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
    def spook_schema(cls) -> dict:
        return {"keys": list, "negate": bool}

    def to_dict(self):
        return {"keys": self._keys, "negate": self._negate}


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
            raise Exception(f"Stage[{self.__class__.__name__}] -> {e}")

    @classmethod
    def spook_schema(cls) -> dict:
        return {"transform_cfg": dict, "targets": dict}

    @classmethod
    def from_dict(cls, d: dict):
        return StageAugmentations(
            transform_cfg=d["transform_cfg"], targets=d["targets"]
        )

    def to_dict(self):
        return {"transform_cfg": self._transform_cfg, "targets": self._targets}
