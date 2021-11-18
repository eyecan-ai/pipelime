from __future__ import annotations

import copy
import multiprocessing
import random
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional, Sequence, Union

import dictquery as dq
import numpy as np
import pydash as py_
import rich
from choixe.spooks import Spook
from loguru import logger
from rich.progress import track
from schema import Or, Schema

from pipelime.sequences.samples import GroupedSample, Sample, SamplesSequence
from pipelime.sequences.stages import SampleStage
from pipelime.tools.idgenerators import IdGenerator, IdGeneratorInteger


class OperationPort(object):
    def __init__(self, schema: dict):
        """Creates an Operation Port with an associated schema (as dict)

        :param schema: port data schema
        :type schema: dict
        """
        self._schema = Schema(schema)

    def match(self, o: "OperationPort"):
        return self._schema.json_schema(0) == o._schema.json_schema(0)

    def is_valid_data(self, o: Any):
        return self._schema.validate(o)

    def __repr__(self):
        return str(self._schema)


class SequenceOperation(ABC):
    """Object representing a generic pipeline operation on a sequence"""

    def __init__(self) -> None:
        pass

    @abstractmethod
    def input_port(self) -> OperationPort:
        raise NotImplementedError()

    @abstractmethod
    def output_port(self) -> OperationPort:
        raise NotImplementedError()

    @abstractmethod
    def __call__(self, x: Any) -> Any:
        p = self.input_port()
        assert p.is_valid_data(x)

    def match(self, x: "SequenceOperation"):
        p0 = self.output_port()
        p1 = x.input_port()
        return p0.match(p1)

    def print(self):
        rich.print(
            {
                "name": self.__class__.__name__,
                "input": self.input_port(),
                "output": self.output_port(),
            }
        )


class MappableOperation(SequenceOperation):
    """Abstract class for generic mappable operations. A mappable operation has the
    following requirements:

    - Takes as input a single `SamplesSequence` `A`
    - Returns a single `SamplesSequence` `B`
    - len(`A`) >= len(`B`)
    - Elements of `A` and `B` have the same order
    - Each element of `B` is the result of a custom function applied on the
      corresponding element in sequence `A`.

    `MappableOperation` automatically takes care of progress bar and multiprocessing, so
    you do not need to write custom multiprocessing logic every time you implement this
    kind of operation.
    """

    def __init__(self, num_workers: int = 0, progress_bar: bool = False) -> None:
        """Constructor for `MappableOperation` subclasses

        :param num_workers: the number of multiprocessing workers, set to -1 to spawn as
        many workers as possible, set to 0 to execute on current process, defaults to 0
        :type num_workers: int, optional
        :param progress_bar: Enable/disable rich progress bar printing on stdout, defaults to False
        :type progress_bar: bool, optional
        """
        super().__init__()
        self._num_workers = num_workers
        self._progress_bar = progress_bar
        self.pbar_desc = f"{self.__class__.__name__}..."

    def input_port(self) -> OperationPort:
        return OperationPort(SamplesSequence)

    def output_port(self) -> OperationPort:
        return OperationPort(SamplesSequence)

    @abstractmethod
    def apply_to_sample(self, sample: Sample) -> Optional[Sample]:
        """Custom function that transforms a sample of sequence `A` to a sample of sequence `B`

        :param sample: A sample of sequence `A`
        :type sample: Sample
        :return: The transformed sample of sequence `B`, None to discard the sample.
        :rtype: Optional[Sample]
        """
        pass

    def __call__(self, x: SamplesSequence) -> SamplesSequence:
        super().__call__(x)
        new_sequence = []
        total = len(x)

        if self._num_workers > 0 or self._num_workers == -1:
            num_workers = None if self._num_workers == -1 else self._num_workers
            pool = multiprocessing.Pool(processes=num_workers)
            new_sequence = pool.imap(self.apply_to_sample, x)

            if self._progress_bar:
                new_sequence = track(new_sequence, total=total)

        else:
            if self._progress_bar:
                x = track(x)

            for sample in x:
                new_sample = self.apply_to_sample(sample)
                new_sequence.append(new_sample)

        new_sequence = [x for x in new_sequence if x is not None]

        return SamplesSequence(samples=new_sequence)

    @classmethod
    def spook_schema(cls) -> dict:
        return {"num_workers": int, "progress_bar": bool}

    def to_dict(self) -> dict:
        return {
            "num_workers": self._num_workers,
            "progress_bar": self._progress_bar,
        }


class OperationSum(SequenceOperation, Spook):
    def __init__(self) -> None:
        """Concatenatas multiple sequences"""
        super().__init__()

    def input_port(self) -> OperationPort:
        return OperationPort([SamplesSequence])

    def output_port(self) -> OperationPort:
        return OperationPort(SamplesSequence)

    def __call__(self, x: Sequence[SamplesSequence]) -> SamplesSequence:
        super().__call__(x)
        new_samples = []
        for s in x:
            new_samples += s.samples
        return SamplesSequence(samples=new_samples)


class OperationIdentity(SequenceOperation, Spook):
    def __init__(self) -> None:
        """No Op"""
        super().__init__()

    def input_port(self) -> OperationPort:
        return OperationPort(any)

    def output_port(self) -> OperationPort:
        return OperationPort(any)

    def __call__(self, x: any) -> any:
        super().__call__(x)
        return x


class OperationResetIndices(SequenceOperation, Spook):  # TODO: unit test!
    def __init__(self, generator: Union[IdGenerator, None] = None) -> None:
        """Reset indices of sample"""
        super().__init__()
        self._generator: Optional[IdGenerator] = (
            generator if generator is not None else IdGeneratorInteger()
        )

    def input_port(self) -> OperationPort:
        return OperationPort(SamplesSequence)

    def output_port(self) -> OperationPort:
        return OperationPort(SamplesSequence)

    def __call__(self, x: SamplesSequence) -> SamplesSequence:
        super().__call__(x)
        for idx in range(len(x)):
            x[idx].id = self._generator.generate()
        return x

    @classmethod
    def spook_schema(cls) -> dict:
        return {"generator": dict}

    @classmethod
    def from_dict(cls, d: dict):
        return OperationResetIndices(generator=Spook.create(d["generator"]))

    def to_dict(self):
        return {"generator": self._generator.serialize()}


class OperationSubsample(SequenceOperation, Spook):
    def __init__(self, factor: Union[int, float]) -> None:
        """Subsample an input sequence elements

        :param factor: if INT is the subsampling factor, if FLOAT is the percentage of input elements to preserve
        :type factor: Union[int, float]
        """
        super().__init__()
        self._factor = factor

    def input_port(self) -> OperationPort:
        return OperationPort(SamplesSequence)

    def output_port(self) -> OperationPort:
        return OperationPort(SamplesSequence)

    def __call__(self, x: SamplesSequence) -> SamplesSequence:
        super().__call__(x)

        new_samples = x.samples.copy()
        if isinstance(self._factor, int):
            new_samples = new_samples[
                :: self._factor
            ]  # Pick an element each `self._factor` elements
        elif isinstance(self._factor, float):
            new_size = int(len(new_samples) * min(max(self._factor, 0), 1.0))
            new_samples = new_samples[:new_size]

        return SamplesSequence(samples=new_samples)

    @classmethod
    def spook_schema(cls) -> dict:
        return {"factor": Or(float, int)}

    def to_dict(self):
        return {"factor": self._factor}


class OperationShuffle(SequenceOperation, Spook):
    def __init__(self, seed=-1) -> None:
        """Shuffle input sequence elements

        :param seed: random seed (-1 for current system time), defaults to -1
        :type seed: int, optional
        """
        super().__init__()
        self._seed = seed

    def input_port(self):
        return OperationPort(SamplesSequence)

    def output_port(self):
        return OperationPort(SamplesSequence)

    def __call__(self, x: SamplesSequence) -> Any:
        super().__call__(x)
        new_data = x.samples.copy()
        random.seed(self._seed if self._seed >= 0 else None)
        random.shuffle(new_data)
        return SamplesSequence(samples=new_data)

    @classmethod
    def spook_schema(cls) -> dict:
        return {"seed": int}

    def to_dict(self):
        return {"seed": self._seed}


class OperationSplits(SequenceOperation, Spook):
    def __init__(self, split_map: Dict[str, float]) -> None:
        """Splits an input sequence in multiple sub-sequences in a key/sequence map

        :param split_map: key/percengate map used as split map. Sum of percentages must be 1.0
        :type split_map: Dict[str, float]
        """
        super().__init__()
        self._split_map = split_map

    def input_port(self):
        return OperationPort(SamplesSequence)

    def output_port(self):
        return OperationPort({str: SamplesSequence})

    def _splits(self, x: SamplesSequence, percentages: Sequence = [0.8, 0.1, 0.1]):
        """Splits sequence in N objects based on a percentage list

        :param percentages: percentages list, defaults to [0.8, 0.1, 0.1]
        :type percentages: list, optional
        :return: list of PandasDatabase
        :rtype: list
        """

        assert np.array(percentages).sum() <= 1.0, "Percentages sum must be <= 1.0"
        sizes = []
        for p in percentages:
            sizes.append(int(len(x) * p))

        sizes[-1] += len(x) - np.array(sizes).sum()

        chunks = []
        current_index = 0
        for s in sizes:
            _samples = x.samples[current_index : current_index + s]
            chunks.append(SamplesSequence(samples=_samples))
            current_index += s
        return tuple(chunks)

    def _splits_as_dict(
        self,
        x: SamplesSequence,
        percentages_dictionary: dict = {"train": 0.9, "test": 0.1},
    ):
        """Splits PandasDatabase in N objects based on a percentage dictionary name/percentage

        :param percentages_dictionary: percentages dictionary, defaults to {'train': 0.9, 'test': 0.1}
        :type percentages_dictionary: dict, optional
        :return: dict of name/PandasDatabase pairs
        :rtype: dict
        """
        names = list(percentages_dictionary.keys())
        percentages = list(percentages_dictionary.values())
        chunks = self._splits(x, percentages=percentages)
        output = {}
        assert len(names) == len(
            chunks
        ), "Len of chunks is different from percentage names number"
        for idx in range(len(names)):
            output[names[idx]] = chunks[idx]
        return output

    def __call__(self, x: SamplesSequence) -> Dict[str, SamplesSequence]:
        super().__call__(x)
        return self._splits_as_dict(x, self._split_map)

    @classmethod
    def spook_schema(cls) -> dict:
        return {"split_map": {str: float}}

    def to_dict(self):
        return {"split_map": self._split_map}


class OperationDict2List(SequenceOperation, Spook):
    def __init__(self) -> None:
        """Converts a Dict of sequences into a play list of sequences"""
        super().__init__()

    def input_port(self):
        return OperationPort({str: SamplesSequence})

    def output_port(self):
        return OperationPort([SamplesSequence])

    def __call__(self, x: Dict[str, SamplesSequence]) -> Sequence[SamplesSequence]:
        super().__call__(x)
        return list(x.values())


class OperationFilterByQuery(MappableOperation, Spook):
    def __init__(self, query: str, **kwargs) -> None:
        """Filter sequence elements based on query string. If sample contains a 'metadata' item
        storing a dict like {'metadata':{'num': 10}}, a query string could be something like
        query = '`metadata.num` > 10' .

        :param query: query string (@see dictquery)
        :type query: str
        """
        super().__init__(**kwargs)
        self._query = query

    def apply_to_sample(self, sample: Sample) -> Optional[Sample]:
        return sample if dq.match(sample, self._query) else None

    @classmethod
    def spook_schema(cls) -> dict:
        return {"query": str, **super().spook_schema()}

    def to_dict(self):
        return {"query": self._query, **super().to_dict()}


# TODO: Replace dictquery with pydash?
class OperationSplitByQuery(SequenceOperation, Spook):
    def __init__(self, query: str) -> None:
        """Splits sequence elements in two sub-sequences based on an input query. The first list
        will contains elements with positve query matches, the second list the negative ones.

        :param query: query string (@see dictquery)
        :type query: str
        """
        self._query = query

    def input_port(self):
        return OperationPort(SamplesSequence)

    def output_port(self):
        return OperationPort([SamplesSequence])

    def __call__(self, x: SamplesSequence) -> Sequence[SamplesSequence]:
        super().__call__(x)
        a = []
        b = []
        for sample in x.samples:
            if dq.match(sample, self._query):
                a.append(sample)
            else:
                b.append(sample)

        return (SamplesSequence(samples=a), SamplesSequence(samples=b))

    @classmethod
    def spook_schema(cls) -> dict:
        return {"query": str}

    def to_dict(self):
        return {"query": self._query}


class OperationSplitByValue(SequenceOperation, Spook):
    def __init__(self, key: str) -> None:
        """Similar to OperationGroupBy, but instead of using grouped samples, it simply
        splits the input dataset into many samples sequences, one for each unique value of
        the specified key

        :param key: the split key, pydash notation
        :type key: str
        """
        super().__init__()
        self._key = key

    def input_port(self) -> OperationPort:
        return OperationPort(SamplesSequence)

    def output_port(self) -> OperationPort:
        return OperationPort(Sequence[SamplesSequence])

    def __call__(self, x: Any) -> Any:
        super().__call__(x)

        groups_map = {}
        for sample in x:
            value = py_.get(sample, self._key)
            if value is not None:
                if value not in groups_map:
                    groups_map[value] = []
                groups_map[value].append(sample)

        for k, samples in groups_map.items():
            groups_map[k] = SamplesSequence(samples)

        keys = sorted(list(groups_map.keys()))
        res = [groups_map[k] for k in keys]
        return res

    @classmethod
    def spook_schema(cls) -> dict:
        return {"key": str}

    def to_dict(self):
        return {"key": self._key}


class OperationGroupBy(SequenceOperation, Spook):
    def __init__(self, field: str, ungrouped: bool = False) -> None:
        """Groups sequence elements accoring to specific field

        :param query: field string (@see pydash deep path notation)
        :type query: str
        :param ungrouped: TRUE to propagate ungrouped samples as separate group, if field is wrong
        and ungrouped is TRUE, a single sample will be propagated. If some samples are lacking in the selected field,
        they will be put in this special group.
        :type ungrouped: bool
        """
        self._field = field.replace("`", "")
        self._ungrouped = ungrouped

    def input_port(self):
        return OperationPort(SamplesSequence)

    def output_port(self):
        return OperationPort(SamplesSequence)

    def __call__(self, x: SamplesSequence) -> Sequence[SamplesSequence]:
        super().__call__(x)

        groups_map = {}
        none_group = []
        for sample in x.samples:
            value = py_.get(sample, self._field)
            if value is not None:
                if value not in groups_map:
                    groups_map[value] = []
                groups_map[value].append(sample)
            else:
                none_group.append(sample)

        out_samples = []
        for k, samples in groups_map.items():
            g = GroupedSample(samples=samples)
            # g['__groupbyvalue__'] = k
            out_samples.append(g)
        if len(none_group) > 0 and self._ungrouped:
            g = GroupedSample(samples=none_group)
            # g['__groupbyvalue__'] = None
            out_samples.append(g)

        return SamplesSequence(samples=out_samples)

    @classmethod
    def spook_schema(cls) -> dict:
        return {"field": str, "ungrouped": bool}

    def to_dict(self):
        return {"field": self._field, "ungrouped": self._ungrouped}


class OperationOrderBy(SequenceOperation, Spook):
    class reversor:
        def __init__(self, obj):
            self.obj = obj

        def __eq__(self, other):
            return other.obj == self.obj

        def __lt__(self, other):
            return other.obj < self.obj

    def __init__(self, order_keys: Sequence[str]) -> None:
        """Order sequence elements based on pydash dict key . like '`metadata.num`'.

        :param order_keys: list of keys to order by
        :type order_keys: Sequence[str]
        """
        self._order_keys = order_keys

    def input_port(self):
        return OperationPort(SamplesSequence)

    def output_port(self):
        return OperationPort(SamplesSequence)

    def _order_pair_from_string(self, v: str):
        if v.startswith("+") or v.startswith("-"):
            return True if v[0] == "-" else False, v[1:]
        else:
            return False, v

    def _order_by(self, x: Sample):
        chunks = []
        for orderby in self._order_keys:
            reverse, orderby_path = self._order_pair_from_string(orderby)
            picked_val = py_.get(x, orderby_path)
            chunks.append(picked_val if not reverse else self.reversor(picked_val))
        return tuple(chunks)

    def __call__(self, x: SamplesSequence) -> SamplesSequence:
        super().__call__(x)
        return SamplesSequence(samples=sorted(x, key=self._order_by))

    @classmethod
    def spook_schema(cls) -> dict:
        return {"order_keys": [str]}

    def to_dict(self):
        return {"order_keys": self._order_keys}


class OperationFilterKeys(MappableOperation, Spook):
    def __init__(self, keys: list, negate: bool = False, **kwargs) -> None:
        """Filter sequence elements by keys

        :param keys: list of keys to preserve
        :type keys: list
        :param negate: TRUE to delete input keys, FALSE delete all but keys
        :type negate: bool
        """
        super().__init__(**kwargs)
        self._keys = keys
        self._negate = negate

    def apply_to_sample(self, sample: Sample) -> Optional[Sample]:
        sample = sample.copy()
        if self._negate:
            to_drop = set(sample.keys()).intersection(self._keys)
        else:
            to_drop = set(sample.keys()).difference(self._keys)
        for k in to_drop:
            del sample[k]
        return sample

    @classmethod
    def spook_schema(cls) -> dict:
        return {"keys": list, "negate": bool, **super().spook_schema()}

    def to_dict(self):
        return {"keys": self._keys, "negate": self._negate, **super().to_dict()}


class OperationFilterByScript(SequenceOperation, Spook):
    def __init__(self, path_or_func: Union[str, Callable]) -> None:
        """Filter sequence elements based on custom python script (or callable). The script has to contain a function
        named `check_sample` with signature `(sample: Sample, sequence: SampleSequence) -> bool` .

        :param path_or_func: python script path, or can be a callable function for On-The-Fly usage
        :type path_or_func: Union[str, Callable]
        """
        if isinstance(path_or_func, str):
            self._path = path_or_func
            self._check_sample = lambda x: True
            if len(self._path) > 0:
                import importlib.util

                spec = importlib.util.spec_from_file_location(
                    "check_sample", self._path
                )
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                self._check_sample = module.check_sample
                assert self._check_sample is not None
            else:
                logger.warning(
                    f"The input script function is empty! Operation performs no checks!"
                )

            self._serializable = True
        elif isinstance(path_or_func, Callable):
            self._path = ""
            self._check_sample = path_or_func
            self._serializable = False
        else:
            raise NotImplementedError(f"Only str|Callable are allowed as input script ")

    def input_port(self):
        return OperationPort(SamplesSequence)

    def output_port(self):
        return OperationPort(SamplesSequence)

    def __call__(self, x: SamplesSequence) -> SamplesSequence:
        super().__call__(x)
        filtered_samples = []
        for sample in x.samples:
            if self._check_sample(sample, x):
                filtered_samples.append(sample)
        return SamplesSequence(samples=filtered_samples)

    @classmethod
    def spook_schema(cls) -> dict:
        return {"path_or_func": str}

    def to_dict(self):
        logger.warning("Operation has on-the-fly mode! No script serialized!")
        return {"path_or_func": self._path}


class OperationMix(SequenceOperation, Spook):
    def __init__(self) -> None:
        """Mixes multiple sequences with same length and disjoint item sets"""
        super().__init__()

    def input_port(self) -> OperationPort:
        return OperationPort([SamplesSequence])

    def output_port(self) -> OperationPort:
        return OperationPort(SamplesSequence)

    def _check_length(sekf, x: Sequence[SamplesSequence]) -> None:
        N = len(x[0])
        for seq in x:
            assert len(seq) == N, "Not all sample sequences have the same length"

    def _check_keys(self, x: Sequence[SamplesSequence]) -> None:
        keys_sets = []
        for i in range(1, len(x)):
            keys_sets.append(set(x[i][0].keys()))
        keys_list = []
        keys_set = set()
        for k_set in keys_sets:
            keys_list.extend(list(k_set))
            keys_set = keys_set.union(set(k_set))
        assert len(keys_set) == len(keys_list)

    def __call__(self, x: Sequence[SamplesSequence]) -> SamplesSequence:
        from rich.progress import track

        super().__call__(x)
        self._check_length(x)
        self._check_keys(x)
        N = len(x[0])
        out = copy.deepcopy(x[0])
        for i in track(range(N)):
            for seq in x:
                out[i].update(seq[i])
        return out


class OperationStage(MappableOperation, Spook):
    """Transforms a `SampleStage` object into a multiprocessing-ready operation
    that is applied on a whole dataset.
    """

    def __init__(self, stage: SampleStage, **kwargs) -> None:
        """Instantiates an `OperationStage` object

        :param stage: the stage object to apply to every sample of a dataset
        :type stage: SampleStage
        """
        super().__init__(**kwargs)
        self._stage = stage

    def apply_to_sample(self, sample: Sample) -> Optional[Sample]:
        return self._stage(sample)

    @classmethod
    def spook_schema(cls) -> dict:
        return {"stage": dict, **super().spook_schema()}

    def to_dict(self) -> dict:
        return {"stage": self._stage.serialize(), **super().to_dict()}

    @classmethod
    def from_dict(cls, d: dict) -> OperationStage:
        stage = Spook.create(d.pop("stage"))
        return cls(stage, **d)
