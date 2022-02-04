from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Union, Collection, Sequence, Mapping, Tuple, Optional
import io
from pathlib import Path
import albumentations as A
from choixe.spooks import Spook
from pipelime.sequences.samples import Sample, FileSystemSample
from pipelime.filesystem.toolkit import FSToolkit


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

    def __call__(self, x: Sample) -> Sample:
        return x


class StageRemap(SampleStage):
    def __init__(self, remap: Mapping[str, str], remove_missing: bool = True):
        """Remaps keys in sample

        :param remap: old_key:new_key dictionary remap
        :type remap: Mapping[str, str]
        :param remove_missing: if TRUE missing keys in remap will be removed in the
            output sample, defaults to True
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
    def __init__(self, key_list: Collection[str], negate: bool = False):
        """Filter sample keys

        :param key_list: list of keys to preserve
        :type key_list: Collection[str]
        :param negate: TRUE to delete input keys, FALSE delete all but keys
        :type negate: bool
        """
        super().__init__()
        self._keys = key_list
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
        return {"key_list": list, "negate": bool}

    def to_dict(self):
        return {"key_list": self._keys, "negate": self._negate}


class StageAugmentations(SampleStage):
    def __init__(self, transform_cfg: Union[dict, str], targets: dict):
        super().__init__()

        self._transform: A.Compose = (
            A.load(
                transform_cfg,
                data_format="json" if transform_cfg.endswith("json") else "yaml",
            )
            if isinstance(transform_cfg, str)
            else A.from_dict(transform_cfg)
        )

        # get an up-to-date description
        self._transform_cfg = A.to_dict(self._transform)

        self._targets = targets
        self._transform.add_targets(self._purge_targets(self._targets))

    def _purge_targets(self, targets: dict):
        # TODO: could it be wrong if targets also contains
        # 'image' or 'mask' (aka default target)?
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


class StageUploadToRemote(SampleStage):
    @dataclass
    class RemoteParams:
        """Remote parameters.

        Args:
            scheme (str): the protocol used, eg, 'file', 's3' etc.
            netloc (str): the ip address and port or 'localhost'.
            base_path (str): the base path for all uploads, eg, the name of the bucket.
            init_args (Mapping[str, str], optional): keyword arguments forwarded to the
                remote initializer. Defaults to {}.
        """

        scheme: str
        netloc: str
        base_path: str
        init_args: Mapping[str, str] = field(default_factory=dict)

    def __init__(
        self,
        remotes: Union[RemoteParams, Sequence[RemoteParams]],
        key_ext_map: Mapping[str, Optional[str]] = {},
    ):
        """Upload sample data to one or more remotes. The uploaded items are then
        managed through a RemoteMetaItem.

        Args:
            remotes (Sequence[RemoteParams]): list of remotes.
            key_ext_map (Mapping[str, str], optional): the item keys to upload and the
                associated file extension to use. If no extension is given, the item is
                pickled. Defaults to {}.
        """
        from pipelime.filesystem.remotes import BaseRemote, create_remote

        def _check_ext(e: Optional[str]) -> str:
            if isinstance(e, str) and len(e) > 0:
                return ("." + e) if e[0] != "." else e
            return ".pickle"

        self._key_ext_map = {k: _check_ext(e) for k, e in key_ext_map.items()}
        self._remotes: Sequence[Tuple[BaseRemote, str]] = []
        for rm in remotes if isinstance(remotes, Sequence) else [remotes]:
            rm_instance = create_remote(rm.scheme, rm.netloc, **rm.init_args)
            if rm_instance is not None:
                self._remotes.append((rm_instance, rm.base_path))

    def __call__(self, x: Sample) -> Sample:
        x = x.copy()
        for k, ext in self._key_ext_map.items():
            if k in x:
                url_list = []
                if (
                    isinstance(x, FileSystemSample)
                    and not x.is_cached(k)
                    and ext == "".join(Path(x.filesmap[k]).suffixes)
                ):
                    for rm, base_path in self._remotes:
                        target = rm.upload_file(x.filesmap[k], base_path)
                        if target is not None:
                            url_list.append(target)
                else:
                    data_stream = io.BytesIO()
                    FSToolkit.store_data_to_stream(data_stream, ext, x[k])

                    data_stream.seek(0, io.SEEK_END)
                    data_size = data_stream.tell()
                    data_stream.seek(0, io.SEEK_SET)

                    for rm, base_path in self._remotes:
                        target = rm.upload_stream(
                            data_stream, data_size, base_path, ext
                        )
                        if target is not None:
                            url_list.append(target)

                # replace item value with the list of remote urls
                x[k] = url_list
        return x
