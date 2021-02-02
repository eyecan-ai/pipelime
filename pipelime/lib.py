import rich
from schema import Or, Schema
import collections
import dictquery as dq
from dataclasses import dataclass
import json
import yaml
from collections.abc import MutableMapping
from abc import ABC,  abstractmethod
from pathlib import Path
from collections import defaultdict
import imghdr
from typing import Any, Dict, Sequence, Tuple, Union
import numpy as np
import imageio
import random


class FSToolkit(object):

    @classmethod
    def tree_from_underscore_notation_files(cls, folder, skip_hidden_files=True):
        """Walk through files in folder generating a tree based on Underscore notation.
        Leafs of abovementioned tree are string representing filenames, inner nodes represent
        keys hierarchy.

        :param folder: target folder
        :type folder: str
        :param skip_hidden_files: TRUE to skip files starting with '.', defaults to True
        :type skip_hidden_files: bool, optional
        :return: dictionary representing multilevel tree
        :rtype: dict
        """

        # Declare TREE structure
        def tree():
            return defaultdict(tree)

        keys_tree = tree()
        folder = Path(folder)
        files = list(sorted(folder.glob('*')))
        for f in files:
            name = f.stem

            if skip_hidden_files:
                if name.startswith('.'):
                    continue

            chunks = name.split('_', maxsplit=1)
            if len(chunks) == 1:
                chunks.append('none')
            p = keys_tree
            for index, chunk in enumerate(chunks):
                if index < len(chunks) - 1:
                    p = p[chunk]
                else:
                    p[chunk] = str(f)

        return dict(keys_tree)

    @classmethod
    def get_file_extension(cls, filename, with_dot=False):
        ext = Path(filename).suffix.lower()
        if not with_dot and len(ext) > 0:
            ext = ext[1:]
        return ext

    @classmethod
    def is_metadata_file(cls, filename: str) -> bool:
        ext = cls.get_file_extension(filename)
        return ext in ['yml', 'json', 'toml', 'tml']

    @classmethod
    def is_file_image(cls, filename: str) -> bool:
        return imghdr.what(filename) is not None

    @classmethod
    def is_file_numpy_array(cls, filename: str) -> bool:
        ext = cls.get_file_extension(filename)
        if ext in ['txt', 'data']:
            try:
                np.loadtxt(filename)
                return True
            except Exception:
                return False
        if ext in ['npy', 'npz']:
            try:
                np.load(filename)
                return True
            except Exception:
                return False
        return False

    @classmethod
    def load_data(cls, filename: str) -> Union[None, np.ndarray, dict]:
        """ Load data from file based on its extension

        :param filename: target filename
        :type filename: str
        :return: Loaded data as array or dict. May return NONE
        :rtype: Union[None, np.ndarray, dict]
        """

        extension = cls.get_file_extension(filename)
        data = None

        if cls.is_file_image(filename):
            data = np.array(imageio.imread(filename))

        if cls.is_file_numpy_array(filename):
            if extension in ['txt']:
                data = np.loadtxt(filename)
            elif extension in ['npy', 'npz']:
                data = np.load(filename)
            if data is not None:
                data = np.atleast_2d(data)

        if cls.is_metadata_file(filename):
            if extension in ['yml']:
                data = yaml.safe_load(open(filename, 'r'))
            elif extension in ['json']:
                data = json.load(open(filename))

        return data


@dataclass
class MetaItem(object):
    source: Any
    source_type: Any


class Sample(MutableMapping):

    def __init__(self) -> None:
        super().__init__()

    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def copy(self):
        pass

    @abstractmethod
    def metaitem(self, key: any) -> MetaItem:
        pass


class PlainSample(Sample):

    def __init__(self, data: dict = None):
        super().__init__()
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
        return PlainSample(data=self._data.copy())

    def metaitem(self, key: any):
        return MetaItem(
            source=None,
            source_type=None
        )


class FileSystemSample(Sample):

    def __init__(self, data_map: dict, lazy: bool = True):
        """ Creates a FileSystemSample starting from a key/filename map

        :param data_map: key/filename map
        :type data_map: dict
        :param lazy: FALSE to preload data (slow), defaults to False
        :type lazy: bool, optional
        """
        super().__init__()
        self._filesmap = data_map
        self._cached = {}
        if not lazy:
            for k in self.keys():
                d = self[k]

    def __getitem__(self, key):
        if key not in self._cached:
            self._cached[key] = FSToolkit.load_data(self._filesmap[key])
        return self._cached[key]

    def __setitem__(self, key, value):
        self._cached[key] = value

    def __delitem__(self, key):
        del self._cached[key]

    def __iter__(self):
        return iter(set.union(set(self._filesmap.keys()), set(self._cached.keys())))

    def __len__(self):
        return len(self._filesmap)

    def copy(self):
        newsample = FileSystemSample(self._filesmap)
        newsample._cached = self._cached.copy()
        return newsample

    def metaitem(self, key: any):
        return MetaItem(
            source=Path(self._filesmap[key]),
            source_type=Path
        )

    # class XXX(dict):

    #     def __init__(self, database: 'GenericSample'):
    #         """ Creates a LazySample, aka a dict with items loaded when called

    #         :param database: [description]
    #         :type database: GenericSample
    #         """
    #         super().__init__()
    #         self._database = database
    #         self._cached = {}
    #         self._keys_map = {}

    #     def copy(self):
    #         newsample = type(self)(database=self._database)
    #         newsample._cached = self._cached.copy()
    #         newsample._keys_map = self._keys_map.copy()
    #         return newsample

    #     def add_key(self, key: Any, reference: str):
    #         self._keys_map[key] = reference

    #     def __contains__(self, key: Any):
    #         return key in self._keys_map

    #     def get_path(self, key: Any) -> Path:
    #         return Path(self._keys_map[key])

    #     def keys(self):
    #         return self._keys_map.keys()

    #     def items(self):
    #         return [(k, self[k]) for k in self.keys()]

    #     def __getitem__(self, key: Any):

    #         if key not in self._keys_map:
    #             raise KeyError

    #         if key not in self._cached:
    #             self._cached[key] = self._database.load_data(self._keys_map[key])

    #         return self._cached[key]

    #     def pop(self, key):
    #         k = self._keys_map[key]
    #         del self._keys_map[key]
    #         return k

    #     def __setitem__(self, key, value):
    #         self._keys_map[key] = value

    #     def __delitem__(self, key):
    #         if key in self._cached:
    #             del self._cached[key]
    #         if key in self._keys_map:
    #             del self._keys_map[key]

    #     def __iter__(self):
    #         return iter(self._cached)

    #     def __len__(self):
    #         return len(self._keys_map)

    #     def _keytransform(self, key):
    #         return key


# class SamplesSequenceAdder(object):

#     def __add__(self, o: Any):
#         if isinstance(o, SamplesSequence) or isinstance(o, type(self)):
#             return type(self)(samples=self._samples + o._samples)
#         else:
#             raise NotImplementedError(f'Cannot apply __add__ to {type(o)}')


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

    # def filter_by_query(self, query: str):
    #     filtered_samples = []
    #     for sample in self._samples:
    #         if dq.match(sample, query):
    #             filtered_samples.append(sample)
    #     return SamplesSequence(samples=filtered_samples)

    # def downsample(self, factor: int):
    #     return SamplesSequence(samples=self._samples[::factor])

    # def fraction(self, percentage: float):
    #     new_size = int(len(self) * min(max(percentage, 0), 1.0))
    #     return SamplesSequence(samples=self._samples[:new_size])

    # def splits(self, percentages: Sequence = [0.8, 0.1, 0.1]):
    #     """Splits sequence in N objects based on a percentage list

    #     :param percentages: percentages list, defaults to [0.8, 0.1, 0.1]
    #     :type percentages: list, optional
    #     :return: list of PandasDatabase
    #     :rtype: list
    #     """
    #     assert np.array(percentages).sum() <= 1.0, "Percentages sum must be <= 1.0"
    #     sizes = []
    #     for p in percentages:
    #         sizes.append(int(len(self) * p))

    #     sizes[-1] += len(self) - np.array(sizes).sum()

    #     chunks = []
    #     current_index = 0
    #     for s in sizes:
    #         _samples = self._samples[current_index:current_index + s]
    #         chunks.append(SamplesSequence(samples=_samples))
    #         current_index += s
    #     return tuple(chunks)

    # def splits_as_dict(self, percentages_dictionary: dict = {'train': 0.9, 'test': 0.1}):
    #     """Splits PandasDatabase in N objects based on a percentage dictionary name/percentage

    #     :param percentages_dictionary: percentages dictionary, defaults to {'train': 0.9, 'test': 0.1}
    #     :type percentages_dictionary: dict, optional
    #     :return: dict of name/PandasDatabase pairs
    #     :rtype: dict
    #     """
    #     names = list(percentages_dictionary.keys())
    #     percentages = list(percentages_dictionary.values())
    #     chunks = self.splits(percentages=percentages)
    #     output = {}
    #     assert len(names) == len(chunks), "Len of chunks is different from percentage names number"
    #     for idx in range(len(names)):
    #         output[names[idx]] = chunks[idx]
    #     return output

    # def shuffle(self, seed=-1):
    #     """Produces a shuffled copy of the original sequence

    #     :param seed: controlled random seed , defaults to -1
    #     :type seed: int, optional
    #     :return: Shuffled copy of the original sequence
    #     :rtype: SamplesSequence
    #     """
    #     new_data = self._samples.copy()
    #     random.seed(seed)
    #     random.shuffle(new_data)
    #     return SamplesSequence(samples=new_data)

    # def __add__(self, o: Any):
    #     if isinstance(o, SamplesSequence):
    #         return SamplesSequence(samples=self._samples + o._samples)
    #     else:
    #         raise NotImplementedError(f'Cannot apply __add__ to {type(o)}')

    # def __mod__(self, o: Any):
    #     if isinstance(o, int):
    #         return self.downsample(o)
    #     elif isinstance(o, float):
    #         return self.fraction(o)
    #     elif isinstance(o, str):
    #         return self.filter_by_query(o)
    #     else:
    #         raise NotImplementedError(f'Cannot apply __mod__ to {type(o)}')

    # def __truediv__(self, o: Any):
    #     if isinstance(o, Tuple):
    #         return self.splits(percentages=o)
    #     elif isinstance(o, Dict):
    #         return self.splits_as_dict(percentages_dictionary=o)
    #     else:
    #         raise NotImplementedError(f'Cannot apply __truediv__ to {type(o)}')


# class SamplesSequenceOp(SamplesSequence, SamplesSequenceAdder):

#     def __init__(self, samples: Sequence[Sample]):
#         super().__init__(samples)


class OpPort(object):

    def __init__(self, schema):
        self._schema = Schema(schema)

    def match(self, o: 'OpPort'):
        return self._schema.json_schema(0) == o._schema.json_schema(0)

    def is_valid_data(self, o: Any):
        return self._schema.validate(o)

    def __repr__(self):
        return str(self._schema)


class SequenceOp(ABC):

    @abstractmethod
    def input_port(self) -> OpPort:
        raise NotImplementedError()

    @abstractmethod
    def output_port(self) -> OpPort:
        raise NotImplementedError()

    @abstractmethod
    def __call__(self, x: Any) -> Any:
        p = self.input_port()
        assert p.is_valid_data(x)

    def match(self, x: 'SequenceOp'):
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


class SequenceOpFactory(object):

    FACTORY_MAP: Dict[str, SequenceOp] = {}

    @classmethod
    def generic_op_schema(cls) -> Schema:
        return Schema({
            'type': str,
            'options': dict
        })

    @classmethod
    def register_op(cls, op: SequenceOp):
        cls.FACTORY_MAP[op.__name__] = op

    @classmethod
    def create(cls, cfg: dict) -> SequenceOp:
        cls.generic_op_schema().validate(cfg)
        _t = cls.FACTORY_MAP[cfg['type']]
        return _t.build_from_dict(cfg)


def op_factory(o):
    SequenceOpFactory.register_op(o)
    return o


@op_factory
class AddOp(SequenceOp):

    def input_port(self) -> OpPort:
        return OpPort([SamplesSequence])

    def output_port(self) -> OpPort:
        return OpPort(SamplesSequence)

    def __call__(self, x: Sequence[SamplesSequence]) -> SamplesSequence:
        super().__call__(x)
        new_samples = []
        for s in x:
            new_samples += s.samples
        return SamplesSequence(samples=new_samples)

    @classmethod
    def op_name(cls) -> str:
        return AddOp.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.op_name(),
            'options': dict
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        super().build_from_dict(d)
        return AddOp()


@op_factory
class SubsampleOp(SequenceOp):
    def __init__(self, factor: Union[int, float]) -> None:
        super().__init__()
        self._factor = factor

    def input_port(self) -> OpPort:
        return OpPort(SamplesSequence)

    def output_port(self) -> OpPort:
        return OpPort(SamplesSequence)

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
        return SubsampleOp.__name__

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
        return SubsampleOp(d['options']['factor'])


@op_factory
class ShuffleOp(SequenceOp):

    def __init__(self, seed=-1) -> None:
        super().__init__()
        self._seed = seed

    def input_port(self):
        return OpPort(SamplesSequence)

    def output_port(self):
        return OpPort(SamplesSequence)

    def __call__(self, x: SamplesSequence) -> Any:
        super().__call__(x)
        new_data = x.samples.copy()
        random.seed(self._seed if self._seed >= 0 else None)
        random.shuffle(new_data)
        return SamplesSequence(samples=new_data)

    @classmethod
    def op_name(cls) -> str:
        return ShuffleOp.__name__

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
        return ShuffleOp(d['options']['seed'])


@op_factory
class SplitsOp(SequenceOp):

    def __init__(self, split_map: Dict[str, float]) -> None:
        super().__init__()
        self._split_map = split_map

    def input_port(self):
        return OpPort(SamplesSequence)

    def output_port(self):
        return OpPort({str: SamplesSequence})

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
        return SplitsOp.__name__

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
        return SplitsOp(d['options']['split_map'])


@op_factory
class Dict2ListOp(SequenceOp):

    def input_port(self):
        return OpPort({str: SamplesSequence})

    def output_port(self):
        return OpPort([SamplesSequence])

    def __call__(self, x: Dict[str, SamplesSequence]) -> Sequence[SamplesSequence]:
        super().__call__(x)
        return list(x.values())

    @classmethod
    def op_name(cls) -> str:
        return Dict2ListOp.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.op_name(),
            'options': dict
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        super().build_from_dict(d)
        return Dict2ListOp()


@op_factory
class FilterByQueryOp(SequenceOp):

    def __init__(self, query: str) -> None:
        self._query = query

    def input_port(self):
        return OpPort(SamplesSequence)

    def output_port(self):
        return OpPort(SamplesSequence)

    def __call__(self, x: SamplesSequence) -> SamplesSequence:
        super().__call__(x)
        filtered_samples = []
        for sample in x.samples:
            if dq.match(sample, self._query):
                filtered_samples.append(sample)
        return SamplesSequence(samples=filtered_samples)

    @classmethod
    def op_name(cls) -> str:
        return FilterByQueryOp.__name__

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
        return FilterByQueryOp(query=d['options']['query'])


@op_factory
class SplitByQueryOp(SequenceOp):

    def __init__(self, query: str) -> None:
        self._query = query

    def input_port(self):
        return OpPort(SamplesSequence)

    def output_port(self):
        return OpPort([SamplesSequence])

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
        return SplitByQueryOp.__name__

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
        return SplitByQueryOp(query=d['options']['query'])


# SequenceOpFactory.register_op(AddOp)
# SequenceOpFactory.register_op(SubsampleOp)
# SequenceOpFactory.register_op(FilterByQueryOp)
