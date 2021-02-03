from pipelime.filesystem.toolkit import FSToolkit
from dataclasses import dataclass
from collections.abc import MutableMapping
from abc import abstractmethod
from pathlib import Path
from typing import Any, Sequence


@dataclass
class MetaItem(object):
    source: Any
    source_type: Any


class Sample(MutableMapping):

    @abstractmethod
    def copy(self):
        pass

    @abstractmethod
    def metaitem(self, key: any) -> MetaItem:
        pass


class PlainSample(Sample):

    def __init__(self, data: dict = None):
        super().__init__()
        self._data = data if data is not None else {}

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value

    def __delitem__(self, key):
        del self._data[key]

    def __iter__(self):
        return iter(self._data.keys())

    def __len__(self):
        return len(self._data)

    def __repr__(self) -> str:
        return str(self._data)

    def copy(self):
        return PlainSample(data=self._data.copy())

    def metaitem(self, key: any):
        return MetaItem(
            source=None,
            source_type=None
        )


class FileSystemSample(Sample):

    def __init__(self, data_map: dict, lazy: bool = True):
        """ Creates a FileSystemSample based on a key/filename map

        :param data_map: key/filename map
        :type data_map: dict
        :param lazy: FALSE to preload data (slow), defaults to False
        :type lazy: bool, optional
        """
        super().__init__()
        self._filesmap = data_map
        self._cached = {}
        if not lazy:
            for k in self.keys():
                d = self[k]

    def __getitem__(self, key):
        if key not in self._cached:
            self._cached[key] = FSToolkit.load_data(self._filesmap[key])
        return self._cached[key]

    def __setitem__(self, key, value):
        self._cached[key] = value

    def __delitem__(self, key):
        del self._cached[key]

    def __iter__(self):
        return iter(set.union(set(self._filesmap.keys()), set(self._cached.keys())))

    def __len__(self):
        return len(self._filesmap)

    def copy(self):
        newsample = FileSystemSample(self._filesmap)
        newsample._cached = self._cached.copy()
        return newsample

    def metaitem(self, key: any):
        return MetaItem(
            source=Path(self._filesmap[key]),
            source_type=Path
        )


class SamplesSequence(Sequence):

    def __init__(self, samples: Sequence[Sample]):
        self._samples = samples

    @property
    def samples(self):
        return self._samples

    def __len__(self) -> int:
        return len(self._samples)

    def __getitem__(self, idx: int):
        if idx >= len(self):
            raise IndexError

        return self._samples[idx]
