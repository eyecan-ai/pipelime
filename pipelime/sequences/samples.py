from schema import Schema
import uuid
from pipelime.filesystem.toolkit import FSToolkit
from dataclasses import dataclass
from collections.abc import MutableMapping
from abc import abstractmethod
from pathlib import Path
from typing import Hashable, Sequence


@dataclass
class MetaItem(object):

    def __init__(self) -> None:
        pass

    @abstractmethod
    def source(self) -> any:
        pass


class MemoryItem(MetaItem):

    def __init__(self) -> None:
        super().__init__()

    def source(self) -> any:
        return None


class FilesystemItem(MetaItem):

    def __init__(self, path: str) -> None:
        super().__init__()
        self._path = Path(path)

    def source(self) -> any:
        return self._path


class Sample(MutableMapping):

    def __init__(self, id: Hashable = None) -> None:
        self._id = id if id is not None else str(uuid.uuid1())

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, v: Hashable):
        self._id = v

    @abstractmethod
    def rename(self, old_key: str, new_key: str):
        pass

    @abstractmethod
    def copy(self):
        pass

    @abstractmethod
    def metaitem(self, key: any) -> MetaItem:
        pass

    def validate(self, schema: Schema, deep: bool = True):
        if deep:
            schema.validate(dict(self))
        else:
            schema.validate({x: None for x in self.keys()})


class GroupedSample(Sample):

    def __init__(self, samples: Sequence[Sample], id: Hashable = None) -> None:
        """ Sample representing a group of basic samples

        :param samples: list of samples to group
        :type samples: Sequence[Sample]
        :param id: hashable value used as id, defaults to None
        :type id: Hashable, optional
        """
        super().__init__(id=id)
        self._samples = samples

    def _merge_dicts(self, ds: Sequence[dict]):
        d = {}
        if len(ds) > 0:
            for k in ds[0].keys():
                d[k] = tuple(d[k] for d in ds)
        return d

    def __getitem__(self, key):
        if len(self._samples) > 0:
            d = [x[key] for x in self._samples]
            if isinstance(d[0], dict):
                d = self._merge_dicts(d)
            return d
        return None

    def __setitem__(self, key, value):
        for x in self._samples:
            x[key] = value

    def __delitem__(self, key):
        for x in self._samples:
            del x[key]

    def __iter__(self):
        for x in self._samples:
            return iter(x.keys())
        return None

    def __len__(self):
        for x in self._samples:
            return len(x)
        return 0

    def __repr__(self) -> str:
        return str(self._samples)

    def copy(self):
        return GroupedSample(samples=self._samples)

    def rename(self, old_key: str, new_key: str):
        for x in self._samples:
            x.rename(old_key=old_key, new_key=new_key)

    def metaitem(self, key: any):
        for x in self._samples:
            return x.metaitem(key)
        return None


class PlainSample(Sample):

    def __init__(self, data: dict = None, id: Hashable = None):
        """ Plain sample (aka a dict wrapper)

        :param data: dictionary data, defaults to None
        :type data: dict, optional
        :param id: hashable value used as id, defaults to None
        :type id: Hashable, optional
        """
        super().__init__(id=id)
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
        return PlainSample(data=self._data.copy(), id=self.id)

    def rename(self, old_key: str, new_key: str):
        if new_key not in self._data and old_key in self._data:
            self._data[new_key] = self._data[old_key]
            del self._data[old_key]

    def metaitem(self, key: any):
        return MemoryItem()


class FileSystemSample(Sample):

    def __init__(self, data_map: dict, lazy: bool = True, id: Hashable = None):
        """ Creates a FileSystemSample based on a key/filename map

        :param data_map: key/filename map
        :type data_map: dict
        :param lazy: FALSE to preload data (slow), defaults to False
        :type lazy: bool, optional
        :param id: hashable value used as id
        :type id: Hashable, optional
        """
        super().__init__(id=id)
        self._filesmap = data_map
        self._cached = {}
        if not lazy:
            for k in self.keys():
                d = self[k]

    @property
    def filesmap(self):
        return self._filesmap

    def is_cached(self, key) -> bool:
        return key in self._cached

    def __getitem__(self, key):
        if not self.is_cached(key):
            self._cached[key] = FSToolkit.load_data(self._filesmap[key])
        return self._cached[key]

    def __setitem__(self, key, value):
        self._cached[key] = value

    def __delitem__(self, key):
        if key in self._cached:
            del self._cached[key]
        if key in self._filesmap:
            del self._filesmap[key]

    def __iter__(self):
        return iter(set.union(set(self._filesmap.keys()), set(self._cached.keys())))

    def __len__(self):
        return len(self._filesmap)

    def copy(self):
        newsample = FileSystemSample(self._filesmap, id=self.id)
        newsample._cached = self._cached.copy()
        return newsample

    def rename(self, old_key: str, new_key: str):
        if new_key not in self._filesmap and old_key in self._filesmap:
            self._filesmap[new_key] = self._filesmap.pop(old_key)
            if old_key in self._cached:
                self._cached[new_key] = self._cached.pop(old_key)

    def metaitem(self, key: any):
        if key in self._filesmap:
            return FilesystemItem(self._filesmap[key])
        else:
            return MemoryItem()

    def validate(self, schema: Schema, deep: bool = True):
        if deep:
            schema.validate(dict(self))
        else:
            schema.validate({x: None for x in self._filesmap.keys()})


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

    def is_normalized(self) -> bool:
        """ Checks for normalization i.e. each sample has to contain same keys.
        !!This method could be very slow if samples are lazy!!

        :return: TRUE if sequence is normalized
        :rtype: bool
        """

        keys_set = None
        for sample in self:
            if keys_set is None:
                keys_set = set(sample.keys())
            else:
                new_keys_set = set(sample.keys())
                if new_keys_set != keys_set:
                    return False
        return True
