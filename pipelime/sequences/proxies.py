from pipelime.sequences.samples import Sample, SamplesSequence
from pipelime.sequences.stages import SampleStage

from typing import Optional
from collections.abc import Set, MutableMapping
from abc import ABC, abstractmethod


class SamplesSequenceProxy(SamplesSequence):
    def __init__(self, source: SamplesSequence, stage: Optional[SampleStage] = None):
        super().__init__(source)
        if stage is not None:
            self.stage = stage


class CachedSamplesSequence(SamplesSequenceProxy):
    class SampleCache(ABC):
        @abstractmethod
        def get_sample(self, idx: int) -> Optional[Sample]:
            ...  # pragma: no cover

        @abstractmethod
        def add_sample(self, idx: int, s: Sample):
            ...  # pragma: no cover

    class EndlessCachePolicy(SampleCache):
        """Store items in cache with no limit."""

        def __init__(self):
            self._cache_map: MutableMapping[int, Sample] = {}

        def get_sample(self, idx: int) -> Optional[Sample]:
            return self._cache_map.get(idx, None)

        def add_sample(self, idx: int, s: Sample):
            self._cache_map[idx] = s

    class BoundedCachePolicy(EndlessCachePolicy):
        def __init__(self, max_cache_size: int):
            """Store up to max_cache_size items in cache.

            Args:
                max_cache_size (int): maximum items to store.
            """
            super().__init__()
            self._max_cache_size = max_cache_size

        def add_sample(self, idx: int, s: Sample):
            if len(self._cache_map) < self._max_cache_size:
                super().add_sample(idx, s)

    def __init__(
        self,
        source: SamplesSequence,
        sample_cache: SampleCache,  # don't make a default, jsonargparse would complain
        forced_keys: Set[str] = set(),
        stage: Optional[SampleStage] = None,
    ):
        """A proxy to cache previous outputs.

        Args:
            source (SamplesSequence): the source sequence.
            sample_cache (SampleCache): the cache of samples.
            forced_keys (Set[str], optional): always access (and load) these keys from
                any retrieved Sample. Defaults to set().
            stage (Optional[SampleStage], optional): a Stage to apply to samples.
                Defaults to None.
        """
        super().__init__(source, stage)
        self._sample_cache = sample_cache
        self._forced_keys = forced_keys

    def __getitem__(self, idx: int) -> Sample:
        item = self._sample_cache.get_sample(idx)

        if item is None:
            item = super().__getitem__(idx)
            for k in self._forced_keys:
                _ = item[k]
            self._sample_cache.add_sample(idx, item)

        return item

    def merge(self, other: "SamplesSequence") -> "SamplesSequence":
        self._cache_map = {}
        return super().merge(other)
