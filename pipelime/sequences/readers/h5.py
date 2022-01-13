from pathlib import Path
from typing import Hashable, Sequence, Union

import h5py
from schema import Optional

from pipelime.h5.toolkit import H5Database, H5ToolKit
from pipelime.sequences.readers.base import BaseReader, ReaderTemplate
from pipelime.sequences.samples import MemoryItem, MetaItem, Sample


class H5Item(MetaItem):
    def __init__(self, item: h5py.Dataset) -> None:
        super().__init__()
        self._item = item

    def source(self) -> any:
        return self._item


class MultiGroup:
    """Manages H5PY groups fusion, if only one group is present it should behave like
    a normal H5py group
    """

    def __init__(self, groups: Sequence[h5py.Group]) -> None:
        self._groups = groups

    def keys(self) -> Sequence[Hashable]:
        keys = set()
        for group in self._groups:
            keys.update(group.keys())
        return list(keys)

    def __contains__(self, key: Hashable) -> bool:
        for group in self._groups:
            if key in group:
                return True
        return False

    def __getitem__(self, key: Hashable) -> h5py.Group:
        for group in reversed(self._groups):
            if key in group:
                return group[key]
        raise KeyError(key)

    def get(self, key: Hashable, getlink: bool = True) -> Union[h5py.Group, any]:
        for group in reversed(self._groups):
            if key in group:
                return group.get(key, getlink=getlink)
        raise KeyError(key)

    def merge(self, other: "MultiGroup") -> "MultiGroup":
        return MultiGroup(self._groups + other._groups)


class H5Sample(Sample):
    def __init__(
        self,
        group: Union[h5py.Group, MultiGroup],
        lazy: bool = True,
        copy_global_items: bool = True,
        id: Hashable = None,
    ):
        """Creates a H5Sample based on a key/filename map

        :param group: h5py Group or MultiGroup
        :type group: Union[h5py.Group, MultiGroup]
        :param lazy: FALSE to preload data (slow), defaults to False
        :type lazy: bool, optional
        :param copy_global_items: TRUE to propagate global item to samples, defaults to
            True
        :type copy_global_items: bool, optional
        :param id: hashable value used as id
        :type id: Hashable, optional
        """
        super().__init__(id=id)
        self._group = MultiGroup([group]) if isinstance(group, h5py.Group) else group
        self._cached = {}
        self._lazy = lazy
        self._copy_global_items = copy_global_items
        self._copy_global_items = copy_global_items
        self._keys = set()
        for key in self._group.keys():
            if self.is_link(key):
                if self._copy_global_items:
                    self._keys.add(key)
            else:
                self._keys.add(key)

        self._cached = {}
        if not lazy:
            for k in self.keys():
                self.get(k)

    def is_cached(self, key) -> bool:
        return key in self._cached

    def __getitem__(self, key):
        if not self.is_cached(key):
            dataset = self._group[key]
            data = H5ToolKit.decode_data(dataset)
            self._cached[key] = data
        return self._cached[key]

    def get_encoding(self, key):
        dataset = self._group[key]
        return H5ToolKit.get_encoding(dataset)

    def is_link(self, key):
        return isinstance(self._group.get(key, getlink=True), h5py.SoftLink)

    def __setitem__(self, key, value):
        self._cached[key] = value

    def __delitem__(self, key):
        if key in self._cached:
            del self._cached[key]

    def __iter__(self):
        return iter(
            self._keys
        )  # iter(set.union(set(self._group.keys()), set(self._cached.keys())))

    def __len__(self):
        return len(self._keys)

    def merge(self, other: "H5Sample") -> "H5Sample":
        merged_groups = self._group.merge(other._group)
        merged_cache = self._cached.copy()
        merged_cache.update(other._cached)
        newsample = H5Sample(merged_groups, id=self.id)
        newsample._cached = merged_cache
        return newsample

    def copy(self):
        newsample = H5Sample(self._group, id=self.id)
        newsample._cached = self._cached.copy()
        return newsample

    def rename(self, old_key: str, new_key: str):
        raise NotImplementedError

    def metaitem(self, key: any):
        if key in self._group:
            return H5Item(self._group[key])
        else:
            return MemoryItem()

    @property
    def skeleton(self) -> dict:
        return {x: None for x in self._group.keys()}

    def flush(self):
        keys = list(self._cached.keys())
        for k in keys:
            del self._cached[k]


class H5Reader(BaseReader):
    DATA_SUBFOLDER = "data"

    def __init__(
        self, filename: str, copy_root_files: bool = True, lazy_samples: bool = True
    ) -> None:

        self._filename = Path(filename)
        self._copy_root_files = copy_root_files
        self._lazy_samples = lazy_samples

        self._h5database = H5Database(filename=self._filename, readonly=True)
        self._h5database.open()

        self._ids = list(sorted(self._h5database.sample_keys()))
        self._root_files_keys = set()

        samples = []
        for idx in range(len(self._ids)):
            group = self._h5database.get_sample_group(
                self._ids[idx], force_create=False
            )

            sample = H5Sample(
                group=group,
                copy_global_items=self._copy_root_files,
                lazy=self._lazy_samples,
                id=idx,
            )
            samples.append(sample)

        if len(samples) > 0:
            sample: H5Sample = samples[0]
            for key in sample:
                if sample.is_link(key):
                    self._root_files_keys.add(key)

        super().__init__(samples=samples)

    def is_root_key(self, key: str):
        return key in self._root_files_keys

    def get_reader_template(self) -> Union[ReaderTemplate, None]:
        """Retrieves the template of the h5 reader, i.e. a mapping
        between sample_key/file_extension and a list of root files keys

        :raises TypeError: If first sample is not a H5Sample
        :return: None if dataset is empty, otherwise a ReaderTemplate
        :rtype: Union[ReaderTemplate, None]
        """

        if len(self) > 0:
            sample = self[0]
            if not isinstance(sample, H5Sample):
                raise TypeError(f"Anomalous sample type found: {type(sample)}")

            extensions_map = {}
            idx_length = len(str(sample.id))
            for key in sample:
                if sample.get_encoding(key) is not None:
                    extensions_map[key] = sample.get_encoding(key)

            return ReaderTemplate(
                extensions_map=extensions_map,
                root_files_keys=list(self._root_files_keys),
                idx_length=idx_length,
            )
        else:
            None

    def flush(self):
        """Clear cache for each internal FileSystemSample"""
        for sample in self:
            sample.flush()

    @classmethod
    def bean_schema(cls) -> dict:
        return {
            "filename": str,
            Optional("copy_root_files"): bool,
            Optional("lazy_samples"): bool,
        }

    @classmethod
    def from_dict(cls, d: dict):
        return H5Reader(
            filename=d.get("filename"),
            copy_root_files=d.get("copy_root_files", True),
            lazy_samples=d.get("lazy_samples", True),
        )

    def to_dict(self) -> dict:
        return {
            "filename": str(self._filename),
            "copy_root_files": self._copy_root_files,
            "lazy_samples": self._lazy_samples,
        }
