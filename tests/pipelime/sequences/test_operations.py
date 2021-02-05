import pydash
import functools
import hashlib
from typing import Dict, Sequence
import pytest
import rich
from schema import Schema
from pipelime.sequences.samples import PlainSample, SamplesSequence
from pipelime.sequences.operations import OperationDict2List, OperationFilterByQuery, OperationGroupBy, OperationIdentity, OperationPort, OperationShuffle, OperationSplitByQuery, OperationSplits, OperationSubsample, OperationSum, SequenceOperation, SequenceOperationFactory


def _plug_test(op: SequenceOperation):
    """ Test what a generic SequenceOperation should do

    :param op: input SequenceOperation
    :type op: SequenceOperation
    """

    assert isinstance(op, SequenceOperation)
    assert isinstance(op.input_port(), OperationPort)
    assert isinstance(op.output_port(), OperationPort)
    assert op.input_port().match(op.input_port())
    assert op.output_port().match(op.output_port())

    reop = op.build_from_dict(op.to_dict())
    assert isinstance(reop, SequenceOperation)
    assert isinstance(op.factory_schema(), Schema)
    op.print()

    factored = SequenceOperationFactory.create(op.to_dict())
    assert isinstance(factored, SequenceOperation)


class TestOperationSum(object):

    def test_sum(self, plain_samples_sequence_generator):

        pairs = [
            (32, 5),
            (32, 1),
            (8, 32),
            (1, 24),
            (0, 64),
            (48, 0)
        ]

        for N, D in pairs:
            datasets = []
            for idx in range(D):
                datasets.append(plain_samples_sequence_generator('d{idx}_', N))

            op = OperationSum()
            _plug_test(op)

            if D > 0:
                out = op(datasets)

                assert isinstance(out, SamplesSequence)
                assert N * D == len(out)
            else:
                with pytest.raises(AssertionError):
                    out = op(datasets)


class TestOperationSubsample(object):

    def test_subsample(self, plain_samples_sequence_generator):

        sizes = [32, 10, 128, 16, 1]
        factors = [2, 0.1, -0.1, 10.2, 10, 20, 1.0, 1]

        for F in factors:
            for N in sizes:
                dataset = plain_samples_sequence_generator('d0_', N)

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


class TestOperationIdentity(object):

    def test_subsample(self, plain_samples_sequence_generator):

        sizes = [32, 10, 128, 16, 1]

        for N in sizes:
            dataset = plain_samples_sequence_generator('d0_', N)

            op = OperationIdentity()
            _plug_test(op)

            out = op(dataset)

            assert out == dataset


class TestOperationShuffle(object):

    def _sign(self, dataset: SamplesSequence):
        names = [x['idx'] for x in dataset.samples]
        return hashlib.md5(bytes('_'.join(names), encoding='utf-8')).hexdigest()

    def test_shuffle(self, plain_samples_sequence_generator):

        sizes = [100, 10, 128, 20]

        for N in sizes:
            dataset = plain_samples_sequence_generator('d0_', N)

            sign = self._sign(dataset)

            op = OperationShuffle(seed=10)
            _plug_test(op)

            out = op(dataset)
            out_sign = self._sign(out)

            assert sign != out_sign, 'for a series of unfortunate events did the two hashes collide?'

            rich.print(sign, "!=", out_sign)

            assert len(dataset) == len(out)


class TestOperationSplits(object):

    def test_splits(self, plain_samples_sequence_generator):

        N = 128
        splits_cfgs = [
            {'split_map': {'train': 0.5, 'test': 0.2, 'val': 0.3}, 'good': True},
            {'split_map': {'a': 0.4, 'b': 0.2, 'c': 0.2, 'd': 0.2}, 'good': True},
            {'split_map': {'a': 0.8, 'b': 0.}, 'good': True},
            {'split_map': {'a': 1.0}, 'good': True},
            {'split_map': {'a': 0.7}, 'good': True},
            {'split_map': {'a': 1.7}, 'good': False},
            {'split_map': {}, 'good': False},
        ]

        for cfg in splits_cfgs:

            good = cfg['good']
            split_map = cfg['split_map']
            expected_splits = len(split_map)
            dataset = plain_samples_sequence_generator('d0_', N)

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
                    idxs = set([x['idx'] for x in d])
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
                d = plain_samples_sequence_generator('d{idx}_', N)
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
        dataset = plain_samples_sequence_generator('d0_', N)

        queries_items = [
            {'query': '`metadata.name` CONTAINS "d0_"', 'expected': N},
            {'query': '`metadata.name` CONTAINS "@IMPOSSIBLE@!S_TRING12"', 'expected': 0},
            {'query': f'`metadata.N` < {N/2}', 'expected': N / 2},
            {'query': '`metadata.name` like "d?_*"', 'expected': N},

        ]

        for item in queries_items:
            query = item['query']
            expected = item['expected']
            op = OperationFilterByQuery(query=query)
            _plug_test(op)

            out = op(dataset)

            assert len(out) == expected


class TestOperationSplitByQuery(object):

    def test_split_by_query(self, plain_samples_sequence_generator):

        N = 128
        dataset = plain_samples_sequence_generator('d0_', N)

        queries_items = [
            {'query': '`metadata.name` CONTAINS "d0_"', 'expected': [N, 0]},
            {'query': '`metadata.name` CONTAINS "@IMPOSSIBLE@!S_TRING12"', 'expected': [0, N]},
            {'query': f'`metadata.N` < {N/2}', 'expected': [N / 2, N / 2]},
            {'query': '`metadata.name` like "d?_*"', 'expected': [N, 0]},

        ]

        for item in queries_items:
            query = item['query']
            expecteds = item['expected']
            op = OperationSplitByQuery(query=query)
            _plug_test(op)

            out = op(dataset)

            assert isinstance(out, Sequence)
            for idx in range(len(out)):
                assert len(out[idx]) == expecteds[idx]

            sumup = functools.reduce(lambda a, b: len(a) + len(b), out)
            sumup_exp = functools.reduce(lambda a, b: a + b, expecteds)

            assert sumup == sumup_exp


class TestOperationGroupBy(object):

    def test_group_by(self, plain_samples_sequence_generator):

        N = 128
        dataset = plain_samples_sequence_generator('d0_', N, heavy_data=False)

        fields_items = [
            {'field': '`metadata.deep.groupby_field`', 'valid': True},
            {'field': '`metadata.even`', 'valid': True},
            {'field': '`odd`', 'valid': True},
            {'field': '`z`', 'valid': False},
        ]

        for item in fields_items:
            field = item['field']
            valid = item['valid']

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
