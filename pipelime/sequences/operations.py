import pydash as py_
import dictquery as dq
import numpy as np
import random
from typing import Any, Dict, Sequence, Union
from pipelime.sequences.samples import GroupedSample, SamplesSequence
from abc import ABC, abstractmethod
from schema import Or, Schema
import rich


class OperationPort(object):

    def __init__(self, schema: dict):
        """ Creates an Operation Port with an associated schema (as dict)

        :param schema: port data schema
        :type schema: dict
        """
        self._schema = Schema(schema)

    def match(self, o: 'OperationPort'):
        return self._schema.json_schema(0) == o._schema.json_schema(0)

    def is_valid_data(self, o: Any):
        return self._schema.validate(o)

    def __repr__(self):
        return str(self._schema)


class SequenceOperation(ABC):
    """Object representing a generic pipeline operation on a sequence
    """

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

    def match(self, x: 'SequenceOperation'):
        p0 = self.output_port()
        p1 = x.input_port()
        return p0.match(p1)

    def print(self):
        rich.print({
            'name': self.__class__.__name__,
            'input': self.input_port(),
            'output': self.output_port()
        })

    @classmethod
    @abstractmethod
    def op_name(cls) -> str:
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def factory_schema(cls) -> Schema:
        raise NotImplementedError()

    @classmethod
    def build_from_dict(cls, d: dict):
        cls.factory_schema().validate(d)

    @abstractmethod
    def to_dict(self) -> dict:
        pass


class SequenceOperationFactory(object):

    FACTORY_MAP: Dict[str, SequenceOperation] = {}

    @classmethod
    def generic_op_schema(cls) -> Schema:
        return Schema({
            'type': str,
            'options': dict
        })

    @classmethod
    def register_op(cls, op: SequenceOperation):
        cls.FACTORY_MAP[op.__name__] = op

    @classmethod
    def create(cls, cfg: dict) -> SequenceOperation:
        cls.generic_op_schema().validate(cfg)
        _t = cls.FACTORY_MAP[cfg['type']]
        return _t.build_from_dict(cfg)


def register_operation_factory(o: SequenceOperation) -> SequenceOperation:
    """ Register a SequenceOperation to the factory

    :param o: operation to register
    :type o: SequenceOperation
    :return: the same operation. It is intent to be used as decorator
    :rtype: SequenceOperation
    """
    SequenceOperationFactory.register_op(o)
    return o


@register_operation_factory
class OperationSum(SequenceOperation):

    def __init__(self) -> None:
        """ Concatenatas multiple sequences
        """
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

    @classmethod
    def op_name(cls) -> str:
        return OperationSum.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.op_name(),
            'options': dict
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        super().build_from_dict(d)
        return OperationSum()

    def to_dict(self):
        return {
            'type': self.op_name(),
            'options': {}
        }


@register_operation_factory
class OperationSubsample(SequenceOperation):
    def __init__(self, factor: Union[int, float]) -> None:
        """ Subsample an input sequence elements

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
            new_samples = new_samples[::self._factor]
        elif isinstance(self._factor, float):
            new_size = int(len(new_samples) * min(max(self._factor, 0), 1.0))
            new_samples = new_samples[:new_size]

        return SamplesSequence(samples=new_samples)

    @classmethod
    def op_name(cls) -> str:
        return OperationSubsample.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.op_name(),
            'options': {
                'factor': Or(float, int)
            }
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        super().build_from_dict(d)
        return OperationSubsample(d['options']['factor'])

    def to_dict(self) -> dict:
        return {
            'type': self.op_name(),
            'options': {
                'factor': self._factor
            }
        }


@register_operation_factory
class OperationShuffle(SequenceOperation):

    def __init__(self, seed=-1) -> None:
        """ Shuffle input sequence elements

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
    def op_name(cls) -> str:
        return OperationShuffle.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.op_name(),
            'options': {
                'seed': int
            }
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        super().build_from_dict(d)
        return OperationShuffle(d['options']['seed'])

    def to_dict(self) -> dict:
        return {
            'type': self.op_name(),
            'options': {
                'seed': self._seed
            }
        }


@register_operation_factory
class OperationSplits(SequenceOperation):

    def __init__(self, split_map: Dict[str, float]) -> None:
        """ Splits an input sequence in multiple sub-sequences in a key/sequence map

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
            _samples = x.samples[current_index:current_index + s]
            chunks.append(SamplesSequence(samples=_samples))
            current_index += s
        return tuple(chunks)

    def _splits_as_dict(self, x: SamplesSequence, percentages_dictionary: dict = {'train': 0.9, 'test': 0.1}):
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
        assert len(names) == len(chunks), "Len of chunks is different from percentage names number"
        for idx in range(len(names)):
            output[names[idx]] = chunks[idx]
        return output

    def __call__(self, x: SamplesSequence) -> Dict[str, SamplesSequence]:
        super().__call__(x)
        return self._splits_as_dict(x, self._split_map)

    @classmethod
    def op_name(cls) -> str:
        return OperationSplits.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.op_name(),
            'options': {
                'split_map': {str: float}
            }
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        super().build_from_dict(d)
        return OperationSplits(d['options']['split_map'])

    def to_dict(self) -> dict:
        return {
            'type': self.op_name(),
            'options': {
                'split_map': self._split_map
            }
        }


@register_operation_factory
class OperationDict2List(SequenceOperation):

    def __init__(self) -> None:
        """ Converts a Dict of sequences into a play list of sequences
        """
        super().__init__()

    def input_port(self):
        return OperationPort({str: SamplesSequence})

    def output_port(self):
        return OperationPort([SamplesSequence])

    def __call__(self, x: Dict[str, SamplesSequence]) -> Sequence[SamplesSequence]:
        super().__call__(x)
        return list(x.values())

    @classmethod
    def op_name(cls) -> str:
        return OperationDict2List.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.op_name(),
            'options': dict
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        super().build_from_dict(d)
        return OperationDict2List()

    def to_dict(self) -> dict:
        return {
            'type': self.op_name(),
            'options': {}
        }


@register_operation_factory
class OperationFilterByQuery(SequenceOperation):

    def __init__(self, query: str) -> None:
        """ Filter sequence elements based on query string. If sample contains a 'metadata' item
        storing a dict like {'metadata':{'num': 10}}, a query string could be something like
        query = '`metadata.num` > 10' .

        :param query: query string (@see dictquery)
        :type query: str
        """
        self._query = query

    def input_port(self):
        return OperationPort(SamplesSequence)

    def output_port(self):
        return OperationPort(SamplesSequence)

    def __call__(self, x: SamplesSequence) -> SamplesSequence:
        super().__call__(x)
        filtered_samples = []
        for sample in x.samples:
            if dq.match(sample, self._query):
                filtered_samples.append(sample)
        return SamplesSequence(samples=filtered_samples)

    @classmethod
    def op_name(cls) -> str:
        return OperationFilterByQuery.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.op_name(),
            'options': {
                'query': str
            }
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        super().build_from_dict(d)
        return OperationFilterByQuery(query=d['options']['query'])

    def to_dict(self) -> dict:
        return {
            'type': self.op_name(),
            'options': {
                'query': self._query
            }
        }


@register_operation_factory
class OperationSplitByQuery(SequenceOperation):

    def __init__(self, query: str) -> None:
        """ Splits sequence elements in two sub-sequences based on an input query. The first list
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

        return (
            SamplesSequence(samples=a),
            SamplesSequence(samples=b)
        )

    @classmethod
    def op_name(cls) -> str:
        return OperationSplitByQuery.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.op_name(),
            'options': {
                'query': str
            }
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        super().build_from_dict(d)
        return OperationSplitByQuery(query=d['options']['query'])

    def to_dict(self) -> dict:
        return {
            'type': self.op_name(),
            'options': {
                'query': self._query
            }
        }


@register_operation_factory
class OperationGroupBy(SequenceOperation):

    def __init__(self, field: str, ungrouped: bool = False) -> None:
        """ Groups sequence elements accoring to specific field

        :param query: field string (@see pydash deep path notation)
        :type query: str
        :param ungrouped: TRUE to propagate ungrouped samples as separate group, if field is wrong
        and ungrouped is TRUE, a single sample will be propagated. If some samples are lacking in the selected field,
        they will be put in this special group.
        :type ungrouped: bool
        """
        self._field = field.replace('`', '')
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
            g['__groupbyvalue__'] = k
            out_samples.append(g)
        if len(none_group) > 0 and self._ungrouped:
            g = GroupedSample(samples=none_group)
            g['__groupbyvalue__'] = None
            out_samples.append(g)

        return SamplesSequence(samples=out_samples)

    @classmethod
    def op_name(cls) -> str:
        return OperationGroupBy.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.op_name(),
            'options': {
                'field': str,
                'ungrouped': bool
            }
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        super().build_from_dict(d)
        return OperationGroupBy(field=d['options']['field'], ungrouped=d['options']['ungrouped'])

    def to_dict(self) -> dict:
        return {
            'type': self.op_name(),
            'options': {
                'field': self._field,
                'ungrouped': self._ungrouped
            }
        }
