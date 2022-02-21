from pipelime.sequences.samples import Sample, SamplesSequence
from pipelime.sequences.stages import SampleStage, StageCompose
from collections import OrderedDict
from bisect import bisect_right

from typing import (
    Optional,
    Union,
    Any,
    MutableMapping,
    Sequence,
    Collection,
    Callable,
)
from abc import ABC, abstractmethod


class CachedSamplesSequence(SamplesSequence):
    class SampleCache(ABC):
        @property
        def get_fn(self) -> Callable[[int], Sample]:
            return self._get_fn

        @get_fn.setter
        def get_fn(self, fn: Callable[[int], Sample]):
            self._get_fn = fn

        @property
        def key_list(self) -> Optional[Collection[str]]:
            return self._key_list

        @key_list.setter
        def key_list(self, kk: Optional[Collection[str]]):
            self._key_list = kk

        def eval_sample(self, idx: int) -> Sample:
            item = self.get_fn(idx)
            for k in self.key_list if self.key_list is not None else item.keys():
                _ = item[k]
            return item

        @abstractmethod
        def get_sample(self, idx: int) -> Sample:
            pass

        @abstractmethod
        def clear_cache(self):
            pass

    class EndlessCachePolicy(SampleCache):
        """Store items in cache with no limit."""

        def __init__(self):
            self.init_cache()

        def get_sample(self, idx: int) -> Sample:
            item = self._cache_map.get(idx, None)
            if item is None:
                item = self.eval_sample(idx)  # NB: use `self` here, NOT `super()`!!!!
                self.add_sample(idx, item)
            return item

        def add_sample(self, idx: int, s: Sample):
            self._cache_map[idx] = s

        def init_cache(self):
            self._cache_map: MutableMapping[int, Sample] = {}

        def clear_cache(self):
            self._cache_map.clear()

    class BoundedCachePolicy(EndlessCachePolicy):
        def __init__(self, max_cache_size: int):
            """Store only the last max_cache_size items in cache.

            Args:
                max_cache_size (int): maximum items to store in the FIFO queue.
            """
            super().__init__()
            self._max_cache_size = max_cache_size

        def add_sample(self, idx: int, s: Sample):
            if self._max_cache_size > 0:
                self._cache_map[idx] = s
                self._cache_map.move_to_end(idx)  # ensure s is the last Sample
                if len(self._cache_map) > self._max_cache_size:
                    self._cache_map.popitem(False)

        def init_cache(self):
            self._cache_map: OrderedDict[int, Sample] = OrderedDict()

    class PersistentCachePolicy(BoundedCachePolicy):
        def __init__(
            self,
            cache_dir: Optional[str],
            compress: Union[bool, int] = False,
            verbose: bool = False,
            clear_cache: bool = False,
            max_buffer_size: int = 0,
        ):
            """Dump items to disk.

            Args:
                cache_dir (Optional[str]): the path to the cache folder; use None to
                    disable caching.
                compress (Union[bool, int], optional): whether to compress the stored
                    data on disk; if an integer [1-9], sets the amount of compression.
                    Defaults to False.
                verbose (bool, optional): whether to print debug messages when data is
                    evaluated. Defaults to False.
                clear_cache (bool, optional): whether to clear the cache before using
                    it, otherwise existing values will be used. Defaults to False.
                max_buffer_size (int, optional): maximum items to store in a FIFO queue
                    on system memory. Defaults to 0.
            """
            super().__init__(max_buffer_size)
            from joblib import Memory

            self._memory = Memory(
                location=cache_dir, compress=compress, verbose=1 if verbose else 0
            )
            self.eval_sample = self._memory.cache(  # type: ignore
                self.eval_sample, ignore=["self"]
            )
            if clear_cache:
                self.clear_cache()

        def clear_cache(self):
            super().clear_cache()
            self._memory.clear()

    def __init__(
        self,
        source: Sequence[Sample],
        sample_cache: SampleCache,  # don't make a default, jsonargparse would complain
        forced_keys: Optional[Collection[str]] = None,
        stage: Optional[SampleStage] = None,
    ):
        """A proxy to cache previous outputs.

        Args:
            source (Sequence[Sample]): the source sequence.
            sample_cache (SampleCache): the cache of samples.
            forced_keys (Optional[Collection[str]], optional): always access (and load)
                these keys from any retrieved Sample before caching; if None, all keys
                are accessed. Defaults to None.
            stage (Optional[SampleStage], optional): a Stage to apply to samples.
                Defaults to None.
        """
        super().__init__(source, stage)
        self._sample_cache = sample_cache
        self._sample_cache.get_fn = super().__getitem__
        self._sample_cache.key_list = forced_keys

    def __getitem__(self, idx: int) -> Sample:
        return self._sample_cache.get_sample(idx)

    def merge(self, other: SamplesSequence) -> SamplesSequence:
        self._sample_cache.clear_cache()
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
        filter_fn: Callable[[Sample], bool],
        stage: Optional[SampleStage] = None,
    ):
        """A filtered view of an input SamplesSequence.

        Args:
            source (SamplesSequence): the SamplesSequence to filter.
            filter_fn (Callable[[Sample], bool]): a callable returning True for any
                valid sample.
            stage (Optional[SampleStage], optional): a Stage to apply to samples.
                Defaults to None.
        """
        if stage is None:
            stage = source.stage
        elif source.stage is not None:
            stage = StageCompose([source.stage, stage])
        super().__init__(list(filter(filter_fn, source.samples)), stage)


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


class SlicedSamplesSequence(SamplesSequence):
    def __init__(
        self,
        source: SamplesSequence,
        start_idx: Optional[int] = None,
        end_idx: Optional[int] = None,
        step: Optional[int] = None,
        stage: Optional[SampleStage] = None,
    ):
        """Extract a slice [start_idx:end_idx:step] from the input sequence.

        Args:
            source (SamplesSequence): the SamplesSequence to slice.
            start_idx (Optional[int], optional): the first index.
                Defaults to None (ie, first element).
            end_idx (Optional[int], optional): the final index.
                Defaults to None (ie, last element).
            step_idx (Optional[int], optional): the slice step.
                Defaults to None (ie, 1).
            stage (Optional[SampleStage], optional): a Stage to apply to the samples.
                Defaults to None.
        """
        if stage is None:
            stage = source.stage
        elif source.stage is not None:
            stage = StageCompose([source.stage, stage])
        super().__init__(source.samples[start_idx:end_idx:step], stage)
