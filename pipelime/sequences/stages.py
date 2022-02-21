from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Union, Collection, Sequence, Mapping, Tuple, Optional, Any, List
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
            x = x.copy()
            to_transform = {}
            for k in x.keys():
                if k in self._targets:
                    to_transform[k] = x[k]

            _transformed = self._transform(**to_transform)
            for k in self._targets.keys():
                x[k] = _transformed[k]

            return x
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


@dataclass
class RemoteParams:
    """Remote parameters.

    :param scheme: the protocol used, eg, 'file', 's3' etc.
    :type scheme: str
    :param netloc: the ip address and port or 'localhost'.
    :type netloc: str
    :param base_path: the base path for all uploads, eg, the name of the bucket.
    :type base_path: str
    :param init_args: keyword arguments forwarded to the remote initializer.
        Defaults to {}.
    :type init_args: Mapping[str, str], optional
    """

    scheme: str
    netloc: str
    base_path: str
    init_args: Mapping[str, Any] = field(default_factory=dict)
    url: str = field(init=False)

    def __post_init__(self):
        from urllib.parse import ParseResult

        ip_and_port = self.netloc.split(":", 1)
        ip_addr = ip_and_port[0]
        port = ip_and_port[1] if len(ip_and_port) > 1 else ""
        if not ip_addr or ip_addr == "localhost":
            ip_addr = "127.0.0.1"
        ip_and_port = f"{ip_addr}:{port}" if port else ip_addr

        self.url = ParseResult(
            scheme=self.scheme,
            netloc=ip_and_port,
            path=self.base_path,
            params="",
            query="",
            fragment="",
        ).geturl()


class StageUploadToRemote(SampleStage):
    def __init__(
        self,
        remotes: Union[RemoteParams, Sequence[RemoteParams]],
        key_ext_map: Mapping[str, Optional[str]],
    ):
        """Upload sample data to one or more remotes. The uploaded items are then
        managed through a RemoteMetaItem.

        :param remotes: the list of remotes.
        :type remotes: Union[RemoteParams, Sequence[RemoteParams]]
        :param key_ext_map: the item keys to upload and the
            associated file extension to use. If no extension is given, the item is
            pickled.
        :type key_list: Mapping[str, Optional[str]]
        """
        from pipelime.filesystem.remotes import BaseRemote, create_remote

        def _check_ext(e: Optional[str]) -> str:
            if isinstance(e, str) and len(e) > 0:
                return ("." + e) if e[0] != "." else e
            return ".pickle"

        self._key_ext_map = {k: _check_ext(e) for k, e in key_ext_map.items()}
        self._remotes: Sequence[Tuple[BaseRemote, str, str]] = []
        for rm in remotes if isinstance(remotes, Sequence) else [remotes]:
            rm_instance = create_remote(rm.scheme, rm.netloc, **rm.init_args)
            if rm_instance is not None:
                self._remotes.append(
                    (
                        rm_instance,
                        rm.base_path,
                        rm.url,
                    )
                )

    def _fssample_shortcut(self, x: Sample, k: str, ext: str) -> Tuple[List[str], bool]:
        url_list = []
        uploaded = False
        if isinstance(x, FileSystemSample) and not x.is_cached(k):
            if FSToolkit.is_remote_file(x.filesmap[k]):
                # read current remote list
                url_list = FSToolkit.load_remote_list(x.filesmap[k])
            elif ext == "".join(Path(x.filesmap[k]).suffixes):
                # just upload the file
                for rm, base_path, _ in self._remotes:
                    target = rm.upload_file(x.filesmap[k], base_path)
                    if target is not None:
                        url_list.append(target)
                uploaded = True
        return url_list, uploaded

    def _upload_to_remotes(self, url_list, sample, key, ext):
        from urllib.parse import urlparse

        # parsed url to easily find duplicates (see below)
        parsed_url_list = [urlparse(u) for u in url_list]
        parsed_url_list = [
            RemoteParams(
                scheme=u.scheme,
                netloc=u.netloc,
                base_path=Path(u.path[1:]).parent.as_posix(),
            ).url
            for u in parsed_url_list
        ]

        data_stream = None
        data_size = 0
        for rm, base_path, rm_url in self._remotes:
            # first check if the remote is already listed
            if rm_url not in parsed_url_list:
                if data_stream is None:
                    item_data = None
                    if not url_list:
                        # the item is not a 'remote' file
                        # however, it can still be a remote list if not bound to a file
                        item_data = sample[key]
                        if FSToolkit.is_remote_list(item_data):
                            url_list = item_data
                            item_data = None

                    # this list come from a remote file or from a 'memory' item
                    if url_list:
                        # manually get the item from the remote
                        item_data = FSToolkit.load_remote_data(url_list)
                        if FSToolkit.is_remote_list(item_data):
                            raise RuntimeError(
                                f"Stage[{self.__class__.__name__}] "
                                "recursive remotes not allowed!"
                            )

                    # store the item as binary blob
                    data_stream = io.BytesIO()
                    FSToolkit.store_data_to_stream(data_stream, ext, item_data)

                    data_stream.seek(0, io.SEEK_END)
                    data_size = data_stream.tell()
                    data_stream.seek(0, io.SEEK_SET)

                target = rm.upload_stream(data_stream, data_size, base_path, ext)
                if target is not None:
                    url_list.append(target)
                    parsed_url_list.append(rm_url)

        return url_list

    def __call__(self, x: Sample) -> Sample:
        x = x.copy()
        for k, ext in self._key_ext_map.items():
            if k in x:
                url_list, uploaded = self._fssample_shortcut(x, k, ext)
                if not uploaded:
                    url_list = self._upload_to_remotes(url_list, x, k, ext)

                # replace item value with the list of remote urls
                x[k] = url_list
        return x


class StageRemoveRemote(SampleStage):
    def __init__(
        self,
        remotes: Union[RemoteParams, Sequence[RemoteParams]],
        key_list: Sequence[str],
    ):
        """Remove a remote in remote lists. To avoid an unintended data loss, the actual
        file is not deleted on the remote.

        :param remotes: the remote to remove.
        :type remotes: Union[RemoteParams, Sequence[RemoteParams]]
        :param key_list: the target item keys.
        :type key_list: Sequence[str]
        """
        if not isinstance(remotes, Sequence):
            remotes = [remotes]
        self._remotes = [rm.url for rm in remotes]
        self._key_list = key_list

    def _as_remote_url(self, url) -> str:
        from urllib.parse import urlparse

        url = urlparse(url)
        return RemoteParams(
            scheme=url.scheme,
            netloc=url.netloc,
            base_path=Path(url.path[1:]).parent.as_posix(),
        ).url

    def __call__(self, x: Sample) -> Sample:
        x = x.copy()
        for k in self._key_list:
            url_list = []
            if (
                isinstance(x, FileSystemSample)
                and not x.is_cached(k)
                and FSToolkit.is_remote_file(x.filesmap[k])
            ):
                url_list = FSToolkit.load_remote_list(x.filesmap[k])
            else:
                data = x[k]
                if FSToolkit.is_remote_list(data):
                    url_list = data
            if url_list:
                x[k] = [
                    u for u in url_list if self._as_remote_url(u) not in self._remotes
                ]
        return x
