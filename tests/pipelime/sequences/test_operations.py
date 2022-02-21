import functools
import hashlib
from itertools import count
from typing import Dict, Optional, Sequence
import uuid

import pydash
import pytest
import rich
from choixe.spooks import Spook

from pipelime.sequences.operations import (
    MappableOperation,
    OperationDict2List,
    OperationFilterByQuery,
    OperationFilterByScript,
    OperationFilterKeys,
    OperationGroupBy,
    OperationIdentity,
    # OperationMix,
    OperationOrderBy,
    OperationPort,
    OperationRemapKeys,
    OperationResetIndices,
    OperationShuffle,
    OperationSplitByQuery,
    OperationSplitByValue,
    OperationSplits,
    OperationStage,
    OperationSubsample,
    OperationSum,
    SequenceOperation,
)
from pipelime.sequences.samples import Sample, SamplesSequence
from pipelime.sequences.stages import StageRemap
from pipelime.tools.idgenerators import IdGeneratorInteger, IdGeneratorUUID


def _plug_test(op: SequenceOperation, check_serialization: bool = True):
    """Test what a generic SequenceOperation should do

    :param op: input SequenceOperation
    :type op: SequenceOperation
    :param check_serialization: True to also check serialization/deserialization
    :type check_serialization: bool
    """

    assert isinstance(op, SequenceOperation)
    assert isinstance(op.input_port(), OperationPort)
    assert isinstance(op.output_port(), OperationPort)
    assert op.input_port().match(op.input_port())
    assert op.output_port().match(op.output_port())

    if check_serialization:
        reop = op.from_dict(op.to_dict())
        assert isinstance(reop, SequenceOperation)
        schema = op.spook_schema()
        assert isinstance(schema, dict) or schema is None
        op.print()

        factored = Spook.create(op.serialize())
        assert isinstance(factored, SequenceOperation)


class TestOperationSum(object):
    def test_sum(self, plain_samples_sequence_generator):

        pairs = [(32, 5), (32, 1), (8, 32), (1, 24), (0, 64), (48, 0)]

        for N, D in pairs:
            datasets = []
            for idx in range(D):
                datasets.append(plain_samples_sequence_generator("d{idx}_", N))

            op = OperationSum()
            _plug_test(op)

            if D > 0:
                out = op(datasets)

                assert isinstance(out, SamplesSequence)
                assert N * D == len(out)
            else:
                assert op.input_port().is_valid_data(datasets)
                out = op(datasets)


class TestOperationSubsample(object):
    def test_subsample(self, plain_samples_sequence_generator):

        sizes = [32, 10, 128, 16, 1]
        factors = [2, 0.1, -0.1, 10.2, 10, 20, 1.0, 1]

        for F in factors:
            for N in sizes:
                dataset = plain_samples_sequence_generator("d0_", N)

                op = OperationSubsample(factor=F)
                _plug_test(op)

                if N > 0:
                    out = op(dataset)

                    assert isinstance(out, SamplesSequence)

                    if isinstance(F, int):
                        if F > 1 and N > 1:
                            assert len(out) < N
                        if F == 1:
                            assert len(out) == N

                    if isinstance(F, float):
                        if 0.0 < F < 1.0:
                            assert len(out) < N

                        if F == 1.0:
                            assert len(out) == N

    def test_subsample_start(self, plain_samples_sequence_generator):

        sizes = [32, 10, 128, 16, 1]
        factors_int = [2, 10, 20, 1]
        factors_float = [0.1, -0.1, 10.2, 1.0]
        starts_int = [-1, 0, 1, 2, 10, 0.5]
        starts_float = [-0.1, 0.0, 0.1, 0.5, 0.9]

        # test with integer inputs
        for F in factors_int:
            for N in sizes:
                for S in starts_int:
                    dataset = plain_samples_sequence_generator("d0_", N)

                    if S < 0:
                        with pytest.raises(Exception):
                            op = OperationSubsample(factor=F, start=S)
                        continue

                    if isinstance(S, float):
                        with pytest.raises(Exception):
                            op = OperationSubsample(factor=F, start=S)
                        continue

                    op = OperationSubsample(factor=F, start=S)
                    _plug_test(op)

                    if N > 0:
                        out = op(dataset)

                        assert isinstance(out, SamplesSequence)

                        if F > 1 and N > 1:
                            assert len(out) < N
                        if F == 1 and S == 0:
                            assert len(out) == N

        # test with float inputs
        for F in factors_float:
            for N in sizes:
                for S in starts_float:
                    dataset = plain_samples_sequence_generator("d0_", N)

                    if S < 0:
                        with pytest.raises(Exception):
                            op = OperationSubsample(factor=F, start=S)
                        continue

                    op = OperationSubsample(factor=F, start=S)
                    _plug_test(op)

                    if N > 0:
                        out = op(dataset)

                        assert isinstance(out, SamplesSequence)

                        op = OperationSubsample(factor=F, start=S)
                        _plug_test(op)

                        if 0.0 < F < 1.0:
                            assert len(out) < N

                        if F == 1.0 and S == 0.0:
                            assert len(out) == N


class TestOperationIdentity(object):
    def test_subsample(self, plain_samples_sequence_generator):

        sizes = [32, 10, 128, 16, 1]

        for N in sizes:
            dataset = plain_samples_sequence_generator("d0_", N)

            op = OperationIdentity()
            _plug_test(op)

            out = op(dataset)

            assert out == dataset


class TestOperationShuffle(object):
    def _sign(self, dataset: SamplesSequence):
        names = [x["idx"] for x in dataset.samples]
        return hashlib.md5(bytes("_".join(names), encoding="utf-8")).hexdigest()

    def test_shuffle(self, plain_samples_sequence_generator):

        sizes = [100, 10, 128, 20]

        for N in sizes:
            dataset = plain_samples_sequence_generator("d0_", N)

            sign = self._sign(dataset)

            op = OperationShuffle(seed=10)
            _plug_test(op)

            out = op(dataset)
            out_sign = self._sign(out)

            assert (
                sign != out_sign
            ), "for a series of unfortunate events did the two hashes collide?"

            rich.print(sign, "!=", out_sign)

            assert len(dataset) == len(out)


class TestOperationResetIndices(object):
    def test_reset(self, plain_samples_sequence_generator):

        generators = [IdGeneratorUUID(), IdGeneratorInteger()]
        N = 32
        for generator in generators:
            dataset = plain_samples_sequence_generator("d0_", N)
            dataset_clone = plain_samples_sequence_generator("d0_", N)

            # Generate common uuid for samples ensuring they are the same
            for sample_index, _ in enumerate(dataset):
                common_id = str(uuid.uuid1())
                dataset[sample_index].id = common_id
                dataset_clone[sample_index].id = common_id

            op = OperationResetIndices(generator=generator)
            _plug_test(op)
            out = op(dataset)

            for idx in range(len(dataset)):
                assert dataset[idx].id != dataset_clone[idx].id

            assert len(dataset) == len(out)


class TestOperationOrderBy(object):
    def _sign(self, dataset: SamplesSequence):
        names = [x["idx"] for x in dataset.samples]
        return hashlib.md5(bytes("_".join(names), encoding="utf-8")).hexdigest()

    def test_orderby(self, plain_samples_sequence_generator):

        N = 32
        dataset = plain_samples_sequence_generator("d0_", N)

        order_items = [
            {"keys": ["reverse_number"], "different": True},
            {"keys": ["+reverse_number"], "different": True},
            {"keys": ["-reverse_number"], "different": False},
            {
                "keys": ["-reverse_number", "metadata.deep.groupby_field"],
                "different": False,
            },
            {
                "keys": ["metadata.deep.groupby_field", "-reverse_number"],
                "different": False,
            },
            {
                "keys": ["metadata.deep.groupby_field", "reverse_number"],
                "different": True,
            },
            {"keys": ["-metadata.deep.groupby_field"], "different": True},
        ]

        for order_item in order_items:
            order_keys = order_item["keys"]
            should_be_different = order_item["different"]

            op = OperationOrderBy(order_keys=order_keys)
            _plug_test(op)
            out = op(dataset)

            if should_be_different:
                assert self._sign(out) != self._sign(dataset)
            else:
                assert self._sign(out) == self._sign(dataset)

            # for idx in range(len(dataset)):
            #     print("Sample", idx)
            #     print(pydash.get(dataset[idx], 'metadata.deep.groupby_field'), dataset[idx]['reverse_number'])
            #     print(pydash.get(out[idx], 'metadata.deep.groupby_field'), out[idx]['reverse_number'])
            #     if should_be_different:
            #         assert dataset[idx].id != out[idx].id
            #     else:
            #         assert dataset[idx].id == out[idx].id

            assert len(dataset) == len(out)


class TestOperationSplits(object):
    def test_splits(self, plain_samples_sequence_generator):

        N = 128
        splits_cfgs = [
            {"split_map": {"train": 0.5, "test": 0.2, "val": 0.3}, "good": True},
            {"split_map": {"a": 0.4, "b": 0.2, "c": 0.2, "d": 0.2}, "good": True},
            {"split_map": {"a": 0.8, "b": 0.0}, "good": True},
            {"split_map": {"a": 1.0}, "good": True},
            {"split_map": {"a": 0.7}, "good": True},
            {"split_map": {"a": 1.7}, "good": False},
            {"split_map": {}, "good": False},
        ]

        for cfg in splits_cfgs:

            good = cfg["good"]
            split_map = cfg["split_map"]
            expected_splits = len(split_map)
            dataset = plain_samples_sequence_generator("d0_", N)

            if good:
                op = OperationSplits(split_map=split_map)
                _plug_test(op)
                out_dict = op(dataset)
                assert expected_splits == len(out_dict)
                assert len(set(out_dict.keys()).difference(set(split_map.keys()))) == 0

                sumup = 0
                for k, d in out_dict.items():
                    sumup += len(d)
                    print("XX", len(d))

                print("SUMP", split_map.keys(), sumup, N)
                assert sumup == N

                idx_maps: Dict[str, set] = {}
                for k, d in out_dict.items():
                    idxs = set([x["idx"] for x in d])
                    idx_maps[k] = idxs

                for k0, _ in idx_maps.items():
                    for k1, _ in idx_maps.items():
                        if k0 != k1:
                            intersection = idx_maps[k0].intersection(idx_maps[k1])
                            assert len(intersection) == 0
            else:
                with pytest.raises(Exception):
                    op = OperationSplits(split_map=split_map)
                    _plug_test(op)
                    out_dict = op(dataset)


class TestOperationDict2List(object):
    def test_shuffle(self, plain_samples_sequence_generator):

        pairs = [
            (10, 5),
            (10, 1),
            (1, 1),
            (1, 10),
            (1, 0),
        ]

        for N, D in pairs:
            datasets = []
            datasets_map = {}
            for idx in range(D):
                d = plain_samples_sequence_generator("d{idx}_", N)
                datasets.append(d)
                datasets_map[str(idx)] = d

            op = OperationDict2List()
            _plug_test(op)

            if D > 0:
                out = op(datasets_map)

                assert isinstance(out, list)
            else:
                with pytest.raises(Exception):
                    out = op(datasets_map)


class TestOperationFilterByQuery(object):
    def test_filter_by_query(self, plain_samples_sequence_generator):

        N = 128
        dataset = plain_samples_sequence_generator("d0_", N)

        queries_items = [
            {"query": '`metadata.name` CONTAINS "d0_"', "expected": N},
            {
                "query": '`metadata.name` CONTAINS "@IMPOSSIBLE@!S_TRING12"',
                "expected": 0,
            },
            {"query": f"`metadata.N` < {N/2}", "expected": N / 2},
            {"query": '`metadata.name` like "d?_*"', "expected": N},
        ]

        for item in queries_items:
            query = item["query"]
            expected = item["expected"]
            op = OperationFilterByQuery(query=query, num_workers=-1)
            _plug_test(op)

            out = op(dataset)

            assert len(out) == expected


class TestOperationSplitByQuery(object):
    def test_split_by_query(self, plain_samples_sequence_generator):

        N = 128
        dataset = plain_samples_sequence_generator("d0_", N)

        queries_items = [
            {"query": '`metadata.name` CONTAINS "d0_"', "expected": [N, 0]},
            {
                "query": '`metadata.name` CONTAINS "@IMPOSSIBLE@!S_TRING12"',
                "expected": [0, N],
            },
            {"query": f"`metadata.N` < {N/2}", "expected": [N / 2, N / 2]},
            {"query": '`metadata.name` like "d?_*"', "expected": [N, 0]},
        ]

        for item in queries_items:
            query = item["query"]
            expecteds = item["expected"]
            op = OperationSplitByQuery(query=query)
            _plug_test(op)

            out = op(dataset)

            assert isinstance(out, Sequence)
            for idx in range(len(out)):
                assert len(out[idx]) == expecteds[idx]

            sumup = functools.reduce(lambda a, b: len(a) + len(b), out)
            sumup_exp = functools.reduce(lambda a, b: a + b, expecteds)

            assert sumup == sumup_exp


class TestOperationSplitByValue:
    def test_split_by_value(self, plain_samples_sequence_generator):
        N = 20
        G = 5
        key = "metadata.deep.groupby_field"
        dataset = plain_samples_sequence_generator("d0_", N, group_each=G)
        op = OperationSplitByValue(key)
        _plug_test(op)
        out = op(dataset)

        assert len(out) == N // G
        total = 0
        for split in out:
            total += len(split)
            target = pydash.get(split[0], key)
            for sample in split:
                assert pydash.get(sample, key) == target
        assert total == len(dataset)


class TestOperationFilterKeys(object):
    def test_filter_keys(self, plain_samples_sequence_generator):

        N = 128
        dataset = plain_samples_sequence_generator("d0_", N)

        keys_to_filter = [
            {"keys": ["idx"], "negate": False},
            {"keys": ["idx"], "negate": True},
            {"keys": ["number", "data0"], "negate": False},
            {"keys": ["number", "data0"], "negate": True},
            {"keys": [], "negate": False},
        ]

        for item in keys_to_filter:
            keys = item["keys"]
            negate = item["negate"]
            op = OperationFilterKeys(keys=keys, negate=negate, num_workers=-1)
            _plug_test(op)
            out = op(dataset)

            assert len(out) == len(dataset)

            for sample in out:
                for key in keys:
                    if not negate:
                        assert key in sample
                    else:
                        assert key not in sample


class TestOperationGroupBy(object):
    def test_group_by(self, plain_samples_sequence_generator):

        N = 128
        dataset = plain_samples_sequence_generator("d0_", N, heavy_data=False)

        fields_items = [
            {"field": "`metadata.deep.groupby_field`", "valid": True},
            {"field": "`metadata.even`", "valid": True},
            {"field": "`odd`", "valid": True},
            {"field": "`z`", "valid": False},
        ]

        for item in fields_items:
            field = item["field"]
            valid = item["valid"]

            for ungrouped in [True, False]:
                op = OperationGroupBy(field=field, ungrouped=ungrouped)
                _plug_test(op)

                out = op(dataset)
                if valid:
                    assert len(out) < len(dataset)

                    counters = {}
                    for sample in out:
                        for k, v in sample.items():
                            rich.print(k, v)
                        for key in sample.keys():
                            if not isinstance(sample[key], dict):
                                if key not in counters:
                                    counters[key] = 0
                                counters[key] += len(sample[key])

                    for k, c in counters.items():
                        print(k, c)
                        assert c == N
                else:
                    if ungrouped:
                        assert len(out) == 1
                    else:
                        assert len(out) == 0


class TestOperationFilterByScript(object):
    def test_filter_by_script(self, plain_samples_sequence_generator, tmpdir):

        N = 128
        dataset = plain_samples_sequence_generator("d0_", N)

        # Bad filename
        with pytest.raises(Exception):
            op = OperationFilterByScript(path="/tmp/script_IMPOSSIBLE_NAME@@!!")

        func = ""
        func += "import numpy as np\n"
        func += "def check_sample(sample, sequence):\n"
        func += " return np.array(sample['number']) % 2 == 0\n"

        script_path = tmpdir.join("custom_script.py")
        with open(script_path, "w") as f:
            f.write(func)

        op = OperationFilterByScript(path_or_func=str(script_path))
        _plug_test(op)
        out = op(dataset)

        assert len(out) < len(dataset)

    def test_filter_by_script_onthefly(self, plain_samples_sequence_generator, tmpdir):

        N = 128
        dataset = plain_samples_sequence_generator("d0_", N)

        def check_sample_onthefly(sample, sequence):
            import numpy as np

            return np.array(sample["number"]) % 2 == 0

        op = OperationFilterByScript(path_or_func=check_sample_onthefly)
        _plug_test(op)
        out = op(dataset)

        assert len(out) < len(dataset)


class TestMappableOperation(object):
    class CountCallback:
        def __init__(self):
            self.counter = 0

        def __call__(self, data: dict) -> None:
            self.counter += 1

    class OperationPlusOne(MappableOperation):
        def __init__(self, key: str, **kwargs) -> None:
            super().__init__(**kwargs)
            self._key = key

        def apply_to_sample(self, sample: Sample) -> Optional[Sample]:
            sample = sample.copy()
            sample[self._key] += 1
            return sample

    class OperationLessThan10(MappableOperation):
        def __init__(self, key: str, **kwargs) -> None:
            super().__init__(**kwargs)
            self._key = key

        def apply_to_sample(self, sample: Sample) -> Optional[Sample]:
            cond = sample[self._key] < 10
            return sample if cond else None

    @pytest.mark.parametrize(
        ("pb", "workers"), ((False, 0), (True, 0), (False, 5), (True, 5), (True, -1))
    )
    def test_mappable_operation(self, plain_samples_sequence_generator, pb, workers):
        N = 32
        key = "number"

        dataset = plain_samples_sequence_generator("d0_", N)
        callback = self.CountCallback()
        op = self.OperationPlusOne(
            key, progress_bar=pb, num_workers=workers, progress_callback=callback
        )

        out = op(dataset)
        _plug_test(op, check_serialization=False)

        # Check callback was called the right number of times
        assert callback.counter == len(dataset)

        expected_numbers = [x[key] + 1 for x in dataset]
        out_numbers = [x[key] for x in out]

        assert isinstance(out, SamplesSequence)
        assert N == len(out)
        for x, y in zip(expected_numbers, out_numbers):
            assert x == y, (expected_numbers, out_numbers)

        callback = self.CountCallback()
        op = self.OperationLessThan10(key, progress_callback=callback)
        out = op(dataset)

        assert len(out) == 10
        assert callback.counter == len(dataset)


class TestOperationStage(object):
    @pytest.mark.parametrize(
        ("pb", "workers"), ((False, 0), (True, 0), (False, 5), (True, 5), (True, -1))
    )
    def test_operation_stage(self, plain_samples_sequence_generator, pb, workers):
        N = 32
        key = "number"
        key_2 = "number_2"

        dataset = plain_samples_sequence_generator("d0_", N)
        stage = StageRemap({key: key_2})
        op = OperationStage(stage, progress_bar=pb, num_workers=workers)
        _plug_test(op, check_serialization=False)
        out = op(dataset)

        assert isinstance(out, SamplesSequence)
        assert N == len(out)
        for x in out:
            assert key_2 in x
            assert key not in x


class TestOperationRemapKeys(object):
    def test_operation_stage(self, plain_samples_sequence_generator):
        N = 32
        key = "number"
        key_2 = "number_2"

        dataset = plain_samples_sequence_generator("d0_", N)

        for remove_missing in [True, False]:
            op = OperationRemapKeys(remap={key: key_2}, remove_missing=remove_missing)
            _plug_test(op, check_serialization=False)
            out = op(dataset)

            assert isinstance(out, SamplesSequence)
            assert N == len(out)
            for x in out:
                assert key_2 in x
                assert key not in x
                if remove_missing:
                    assert len(x.keys()) == 1
                else:
                    assert len(x.keys()) > 1
