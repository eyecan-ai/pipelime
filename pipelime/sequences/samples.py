import uuid
from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Hashable, Sequence, MutableMapping, Union
import functools
from pipelime.filesystem.toolkit import FSToolkit


@dataclass
class MetaItem(object):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def source(self) -> Any:
        pass


class MemoryItem(MetaItem):
    def __init__(self) -> None:
        super().__init__()

    def source(self) -> Any:
        return None


class FileSystemItem(MetaItem):
    def __init__(self, path: str) -> None:
        super().__init__()
        self._path = Path(path)

    def source(self) -> Path:
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
    def copy(self) -> "Sample":
        pass

    @abstractmethod
    def metaitem(self, key: Any) -> MetaItem:
        pass

    @abstractmethod
    def merge(self, other: "Sample") -> "Sample":
        pass

    @property
    def skeleton(self) -> dict:
        return {x: None for x in self.keys()}

    def flush(self):
        pass


class GroupedSample(Sample):
    def __init__(self, samples: Sequence[Sample], id: Hashable = None) -> None:
        """Sample representing a group of basic samples

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

    def merge(self, other: "GroupedSample") -> "GroupedSample":
        samples = self._samples
        others_samples = other._samples
        if len(samples) != len(others_samples):
            raise ValueError("Cannot merge samples with different lengths")
        merged_samples = [x.merge(y) for x, y in zip(samples, others_samples)]
        return GroupedSample(samples=merged_samples)

    def copy(self):
        return GroupedSample(samples=self._samples)

    def rename(self, old_key: str, new_key: str):
        for x in self._samples:
            x.rename(old_key=old_key, new_key=new_key)

    def metaitem(self, key: Any):
        for x in self._samples:
            return x.metaitem(key)
        return None


class PlainSample(Sample):
    def __init__(self, data: dict = None, id: Hashable = None):
        """Plain sample (aka a dict wrapper)

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

    def merge(self, other: "PlainSample") -> "PlainSample":
        new_data = self._data.copy()
        new_data.update(other._data.copy())
        return PlainSample(data=new_data, id=self.id)

    def copy(self):
        return PlainSample(data=self._data.copy(), id=self.id)

    def rename(self, old_key: str, new_key: str):
        if new_key not in self._data and old_key in self._data:
            self._data[new_key] = self._data[old_key]
            del self._data[old_key]

    def metaitem(self, key: Any):
        return MemoryItem()


class FileSystemSample(Sample):
    def __init__(
        self, data_map: MutableMapping[str, str], lazy: bool = True, id: Hashable = None
    ):
        """Creates a FileSystemSample based on a key/filename map

        :param data_map: key/filename map
        :type data_map: dict
        :param lazy: FALSE to preload data (slow), defaults to False
        :type lazy: bool, optional
        :param id: hashable value used as id
        :type id: Hashable, optional
        """
        super().__init__(id=id)
        self._filesmap: MutableMapping[str, str] = data_map
        self._cached = {}
        if not lazy:
            for k in self.keys():
                self.get(k)

    @property
    def filesmap(self):
        return self._filesmap

    def is_cached(self, key) -> bool:
        return key in self._cached

    def __contains__(self, o: object) -> bool:
        return o in self._cached or o in self._filesmap

    def __getitem__(self, key):
        if not self.is_cached(key):
            self._cached[key] = FSToolkit.load_data(self._filesmap[key])
        return self._cached[key]

    def __setitem__(self, key, value):
        self._cached[key] = value

    def __delitem__(self, key):
        self._cached.pop(key, None)
        self._filesmap.pop(key, None)

    def __iter__(self):
        return iter(set.union(set(self._filesmap.keys()), set(self._cached.keys())))

    def __len__(self):
        return len(set.union(set(self._filesmap.keys()), set(self._cached.keys())))

    def merge(self, other: "FileSystemSample") -> "FileSystemSample":
        new_filesmap = self._filesmap.copy()
        new_cache = self._cached.copy()

        new_filesmap.update(other._filesmap)
        new_cache.update(other._cached)

        newsample = FileSystemSample(new_filesmap, id=self.id)
        newsample._cached = new_cache
        return newsample

    def copy(self):
        newsample = FileSystemSample(self._filesmap.copy(), id=self.id)
        newsample._cached = self._cached.copy()
        return newsample

    def rename(self, old_key: str, new_key: str):
        if new_key not in self._filesmap and old_key in self._filesmap:
            self._filesmap[new_key] = self._filesmap.pop(old_key)
            if old_key in self._cached:
                self._cached[new_key] = self._cached.pop(old_key)
        if new_key not in self._cached and old_key in self._cached:
            self._cached[new_key] = self._cached.pop(old_key)

    def metaitem(self, key: Any):
        if key in self._filesmap:
            return FileSystemItem(self._filesmap[key])
        else:
            return MemoryItem()

    @property
    def skeleton(self) -> dict:
        return {x: None for x in self._filesmap.keys()}

    def flush(self, keys: Sequence[str] = None):
        if keys is None:
            keys = list(self._cached.keys())
        for k in keys:
            del self._cached[k]

    def update(self, other: Mapping[str, Any]) -> None:
        if isinstance(other, FileSystemSample):
            self.filesmap.update(other.filesmap)
            for k in other.keys():
                if other.is_cached(k):
                    self[k] = other[k]
        else:
            super().update(other)


class SamplesSequence(Sequence):
    # ⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️
    # Removed type hinting for stage argument, which resulted in circular import
    # between pipelime.sequences.stages and pipelime.sequences.samples modules

    # Refactoring is needed to decouple these classes or to reorganize these modules
    # to avoid this circular dependency!
    # ⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️

    def __init__(self, samples: Sequence[Sample], stage=None):
        """Constructor for `SamplesSequence`

        :param samples: A sequence of samples that this SampleSequence will contain
        :type samples: Sequence[Sample]
        :param stage: A stage to apply to each sample of the sequence when the
        __getitem__ is called, if set to `None` no stage is applied, defaults to None
        :type stage: Optional[SampleStage], optional
        """
        self._samples = samples

        # This import here is due to circular dependency 💀💀💀 !!
        from pipelime.sequences.stages import StageIdentity

        self._stage = StageIdentity() if stage is None else stage

    @property
    def samples(self):
        return self._samples

    @property
    def stage(self):
        return self._stage

    @stage.setter
    def stage(self, stage):
        # This import here is due to circular dependency 💀💀💀 !!
        from pipelime.sequences.stages import SampleStage

        assert isinstance(stage, SampleStage)
        self._stage = stage

    @samples.setter
    def samples(self, samples: Sequence[Sample]):
        self._samples = samples

    def __len__(self) -> int:
        return len(self._samples)

    def __getitem__(self, idx: int) -> Sample:
        if idx >= len(self):
            raise IndexError

        return self._stage(self._samples[idx])

    def is_normalized(self) -> bool:
        """Checks for normalization i.e. each sample has to contain same keys.
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

    def best_zfill(self) -> int:
        """Computes the best zfill for integer indexing

        :return: zfill values (maximum number of digits based on current size)
        :rtype: int
        """
        return len(str(len(self)))

    @classmethod
    def purge_id(cls, id: Hashable) -> Union[int, str]:
        try:
            return int(id)  # type: ignore
        except Exception:
            return str(id)

    def merge(self, other: "SamplesSequence") -> "SamplesSequence":
        new_samples = [x.merge(y) for x, y in zip(self, other)]
        return SamplesSequence(samples=new_samples)

    @classmethod
    def merge_sequences(
        cls, sequences: Sequence["SamplesSequence"]
    ) -> "SamplesSequence":
        """Merges multiple sequences into one

        :param sequences: sequences to merge
        :type sequences: SamplesSequence
        :return: merged sequence
        :rtype: SamplesSequence
        """
        return functools.reduce(lambda x, y: x.merge(y), sequences)
