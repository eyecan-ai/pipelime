from pipelime.sequences.samples import Sample, SamplesSequence
from pipelime.sequences.stages import SampleStage, StageCompose
from bisect import bisect_right

from typing import (
    Optional,
    Union,
    Any,
    Set,
    MutableMapping,
    Sequence,
    Collection,
    Callable,
)
from abc import ABC, abstractmethod


class CachedSamplesSequence(SamplesSequence):
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
        source: Sequence[Sample],
        sample_cache: SampleCache,  # don't make a default, jsonargparse would complain
        forced_keys: Set[str] = set(),
        stage: Optional[SampleStage] = None,
    ):
        """A proxy to cache previous outputs.

        Args:
            source (Sequence[Sample]): the source sequence.
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

    def merge(self, other: SamplesSequence) -> SamplesSequence:
        self._cache_map = {}
        return super().merge(other)


class ConcatSamplesSequence(SamplesSequence):
    def __init__(
        self, sources: Sequence[SamplesSequence], stage: Optional[SampleStage] = None
    ):
        """Concatenate a sequence of SamplesSequence.

        Args:
            sources (Sequence[SamplesSequence]): the SamplesSequence to concatenate.
            stage (Optional[SampleStage], optional): a Stage to apply to samples.
                Defaults to None.
        """
        concat_samples: Sequence[Sample] = []
        self._src_stages: Sequence[SampleStage] = []
        self._seq_length: Sequence[int] = [0]
        for sseq in sources:
            concat_samples += sseq.samples
            self._src_stages.append(sseq.stage)
            self._seq_length.append(self._seq_length[-1] + len(sseq))

        super().__init__(concat_samples, stage)

    def __getitem__(self, idx: int) -> Sample:
        if idx >= len(self):
            raise IndexError
        sample = self._samples[idx]
        seq_id = bisect_right(self._seq_length, idx) - 1

        return self._stage(self._src_stages[seq_id](sample))


class FilteredSamplesSequence(SamplesSequence):
    def __init__(
        self,
        source: SamplesSequence,
        filter: Union[Collection[int], Callable[[int], bool]],
        stage: Optional[SampleStage] = None,
    ):
        """A filtered view of an input SamplesSequence.

        Args:
            source (SamplesSequence): the SamplesSequence to filter.
            filter (Union[Collection[int], Callable[[int], bool]]): a collection of
                valid indixes or a callable returning True for any valid index.
            stage (Optional[SampleStage], optional): a Stage to apply to samples.
                Defaults to None.
        """
        fn = filter if isinstance(filter, Callable) else lambda idx: idx in filter
        filterd_samples = [source.samples[idx] for idx in range(len(source)) if fn(idx)]

        if stage is None:
            stage = source.stage
        elif source.stage is not None:
            stage = StageCompose([source.stage, stage])

        super().__init__(filterd_samples, stage)


class SortedSamplesSequence(SamplesSequence):
    def __init__(
        self,
        source: SamplesSequence,
        key_fn: Callable[[Sample], Any],
        stage: Optional[SampleStage] = None,
    ):
        """A sorted view of an input SamplesSequence.

        Args:
            source (SamplesSequence): the SamplesSequence to sort.
            key_fn (Callable[[Sample], Any]): the key function to compare Samples. Use
                `functools.cmp_to_key` to convert a compare function, ie, accepting two
                arguments, to a key function.
            stage (Optional[SampleStage], optional): a Stage to apply to samples.
                Defaults to None.
        """
        if stage is None:
            stage = source.stage
        elif source.stage is not None:
            stage = StageCompose([source.stage, stage])
        super().__init__(sorted(source.samples, key=key_fn), stage)
