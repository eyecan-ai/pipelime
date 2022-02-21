from pipelime.sequences.samples import Sample, PlainSample, SamplesSequence
from pipelime.sequences.stages import StageKeysFilter
import pipelime.sequences.proxies as sp

from typing import Sequence, Tuple, Set


class SideEffectSample(PlainSample):
    def get_bypass(self, key):
        return self._data.get(key, None)

    def __getitem__(self, key):
        if self._data[key] > 0:
            self._data[key] *= -1
        return self._data[key]

    def merge(self, other: "SideEffectSample") -> "SideEffectSample":
        new_data = self._data.copy()
        new_data.update(other._data.copy())
        return SideEffectSample(data=new_data, id=self.id)

    def copy(self):
        return SideEffectSample(data=self._data.copy(), id=self.id)


class TestSequenceProxy:
    def _make_random_source(self, max_idx: int):
        import random

        def rnd_fn():
            return random.randint(0, 100)

        return SamplesSequence(
            samples=[
                PlainSample(
                    {
                        "idx": i,
                        "data_1": rnd_fn(),
                        "data_2": rnd_fn(),
                        "data_3": rnd_fn(),
                        "data_4": rnd_fn(),
                    }
                )
                for i in range(0, max_idx)
            ],
            stage=StageKeysFilter(["idx", "data_1", "data_2", "data_3"]),
        )

    def _make_cached_sequence(
        self, sample_cache: sp.CachedSamplesSequence.SampleCache, max_idx: int
    ) -> Tuple[sp.CachedSamplesSequence, Set[str], Set[str], Set[str], Set[str]]:
        source_sseq = SamplesSequence(
            samples=[
                SideEffectSample(
                    {"data_1": i, "data_2": i * 2, "data_3": i * 3, "data_4": i * 4}
                )
                for i in range(1, max_idx)
            ],
            stage=StageKeysFilter(["data_1", "data_2", "data_3"]),
        )
        cached_sseq = sp.CachedSamplesSequence(
            source=source_sseq,
            sample_cache=sample_cache,
            forced_keys={"data_1"},
            stage=StageKeysFilter(["data_1", "data_2"]),
        )

        forced_keys = {"data_1"}
        last_stage_keys = forced_keys | {"data_2"}
        hidden_stage_keys = last_stage_keys | {"data_3"}
        all_keys = hidden_stage_keys | {"data_4"}
        return cached_sseq, all_keys, hidden_stage_keys, last_stage_keys, forced_keys

    def _check_samples(
        self,
        kset: Set[str],
        ksrc: Set[str],
        sseq: Sequence[Sample],
        negative_keys: Set[str],
    ):
        for s in sseq:
            for k in kset:
                if k in negative_keys:
                    assert s.get_bypass(k) < 0  # type: ignore
                else:
                    assert s.get_bypass(k) > 0  # type: ignore
            for k in ksrc - kset:
                assert s.get_bypass(k) is None  # type: ignore

    def _check_cache_buffer(
        self, sseq: sp.CachedSamplesSequence, max_idx: int, cached_idx: Set[int]
    ):
        for idx in range(1, max_idx):
            item = sseq._sample_cache._cache_map.get(idx)  # type: ignore
            if idx in cached_idx:
                assert item is not None
            else:
                assert item is None

    def test_endless_cache_proxy(self):
        max_idx = 5
        sseq, k0, k1, k2, fk = self._make_cached_sequence(
            sp.CachedSamplesSequence.EndlessCachePolicy(), max_idx
        )

        # raw source
        self._check_samples(k0, k0, sseq.samples.samples, set())  # type: ignore
        # copy after first stage
        self._check_samples(k1, k0, sseq.samples, set())
        # copy after second stage, then accessed and cached
        self._check_samples(k2, k0, sseq, fk)
        # these have never been accessed
        self._check_samples(k1, k0, sseq.samples, set())

        # all samples have been cached
        self._check_cache_buffer(sseq, max_idx, {i for i in range(len(sseq))})

    def test_bounded_cache_proxy(self):
        cache_size = 2
        max_idx = 5
        sseq, k0, k1, k2, fk = self._make_cached_sequence(
            sp.CachedSamplesSequence.BoundedCachePolicy(cache_size), max_idx
        )

        # raw source
        self._check_samples(k0, k0, sseq.samples.samples, set())  # type: ignore
        # copy after first stage
        self._check_samples(k1, k0, sseq.samples, set())
        # copy after second stage, then accessed and cached
        self._check_samples(k2, k0, sseq, fk)
        # these have never been accessed
        self._check_samples(k1, k0, sseq.samples, set())

        # only last samples have been cached
        self._check_cache_buffer(
            sseq, max_idx, {i for i in range(len(sseq) - cache_size, len(sseq))}
        )

    def test_persistent_cache_proxy(self, tmp_path):
        def make_cache_test(cache_name):
            cache_dir = tmp_path / cache_name
            cache_dir.mkdir(parents=True, exist_ok=True)

            buffer_size = 2
            max_idx = 5
            sseq, k0, k1, k2, fk = self._make_cached_sequence(
                sp.CachedSamplesSequence.PersistentCachePolicy(
                    cache_dir=cache_dir, max_buffer_size=buffer_size, verbose=True
                ),
                max_idx,
            )

            # raw source
            self._check_samples(k0, k0, sseq.samples.samples, set())  # type: ignore
            # copy after first stage
            self._check_samples(k1, k0, sseq.samples, set())
            # copy after second stage, then accessed and cached
            self._check_samples(k2, k0, sseq, fk)
            # these have never been accessed
            self._check_samples(k1, k0, sseq.samples, set())

            # only last samples have been cached
            self._check_cache_buffer(
                sseq, max_idx, {i for i in range(len(sseq) - buffer_size, len(sseq))}
            )

            return cache_dir

        cache0_full_path = make_cache_test("cache0")
        cache1_full_path = make_cache_test("cache1")
        make_cache_test("cache1")

        from filecmp import dircmp
        from pathlib import Path
        import json
        import joblib

        def file_diff(dcmp: dircmp):
            assert len(dcmp.left_only) == 0
            assert len(dcmp.right_only) == 0
            left_path, right_path = Path(dcmp.left), Path(dcmp.right)
            for f in dcmp.common_files:
                left_f = left_path / Path(f)
                right_f = right_path / Path(f)
                if f == "func_code.py":
                    assert f in dcmp.same_files
                elif f == "metadata.json":
                    with left_f.open() as file:
                        left_data = json.load(file)
                        assert "input_args" in left_data
                    with right_f.open() as file:
                        right_data = json.load(file)
                        assert "input_args" in right_data
                    assert left_data["input_args"] == right_data["input_args"]
                else:
                    assert f == "output.pkl"
                    left_obj = joblib.load(left_f)
                    right_obj = joblib.load(right_f)
                    assert isinstance(left_obj, SideEffectSample)
                    assert isinstance(right_obj, SideEffectSample)
                    for k in left_obj.keys() | right_obj.keys():
                        assert left_obj.get_bypass(k) == right_obj.get_bypass(k)
            for sub_dcmp in dcmp.subdirs.values():
                file_diff(sub_dcmp)

        file_diff(dircmp(str(cache0_full_path), str(cache1_full_path)))

    def test_concat_seq_proxy(self):
        n_seq = 3
        n_items = 5
        srcs = [self._make_random_source(n_items) for i in range(n_seq)]
        cat_sseq = sp.ConcatSamplesSequence(srcs, StageKeysFilter(["data_1", "data_2"]))

        out_iter = iter(cat_sseq)
        for src_seq in srcs:
            for item in src_seq:
                out_item = next(out_iter)
                for k, v in out_item.items():
                    assert k in item
                    assert v == item[k]

    def test_filtered_seq_proxy(self):
        n_items = 5
        src = self._make_random_source(n_items)

        def filter_fn(x: Sample) -> bool:
            return x["idx"] in (2, 4)

        flt_sseq = sp.FilteredSamplesSequence(
            src, filter_fn, StageKeysFilter(["data_1", "data_2"])
        )

        assert len(flt_sseq) == 2
        flt_iter = iter(flt_sseq)
        for src_item in src:
            if filter_fn(src_item):
                flt_item = next(flt_iter)
                for k, v in flt_item.items():
                    assert k in src_item
                    assert v == src_item[k]

    def test_sorted_seq_proxy(self):
        n_items = 5
        src = self._make_random_source(n_items)

        def keyfn(x: Sample):
            return x["data_1"]

        srt_sseq = sp.SortedSamplesSequence(
            src, keyfn, StageKeysFilter(["data_1", "data_2"])
        )

        sorted_src = sorted(src.samples, key=keyfn)
        for src_item, out_item in zip(sorted_src, srt_sseq):
            for k, v in out_item.items():
                assert k in src_item
                assert v == src_item[k]

    def test_sliced_seq_proxy(self):
        n_items = 10
        src = self._make_random_source(n_items)

        slc_sseq = sp.SlicedSamplesSequence(
            src, 2, 9, 3
        )

        sliced_src = src.samples[2:9:3]
        assert len(slc_sseq) == len(sliced_src)
        for src_item, out_item in zip(sliced_src, slc_sseq):
            for k, v in out_item.items():
                assert k in src_item
                assert v == src_item[k]
