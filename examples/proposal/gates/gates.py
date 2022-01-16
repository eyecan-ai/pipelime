from abc import ABC, abstractmethod
from typing import Any, Sequence
from pipelime.sequences.samples import PlainSample, Sample, SamplesSequence
import rich
import inspect


class GNode(ABC):
    def __init__(self, id: str = None):
        self._id = id


class Gate(GNode):
    def __init__(self, id: str = None):
        super().__init__(id=id)

    @property
    def id(self) -> str:
        return self._id

    @abstractmethod
    def __call__(self, x: Sample) -> Sample:
        raise NotImplementedError()


class GateNN(GNode):
    def __init__(self, gates: Sequence[Gate], id: str = None):
        super().__init__(id=id)
        self._gates = gates

    def __call__(self, x: SamplesSequence) -> SamplesSequence:
        out_samples = []
        for sample_index, sample in enumerate(x):
            gate = self._gates[sample_index % len(self._gates)]
            out_samples.append(gate(sample))
        return SamplesSequence(samples=out_samples)


class Gate1N(GNode):
    def __init__(self, id: str = None):
        super().__init__(id=id)

    @abstractmethod
    def __call__(self, x: Sample) -> SamplesSequence:
        raise NotImplementedError()


class GateN1(GNode):
    def __init__(self, id: str = None):
        super().__init__(id=id)

    @abstractmethod
    def __call__(self, x: SamplesSequence) -> Sample:
        raise NotImplementedError()


###############################################################################
###############################################################################
###############################################################################
###############################################################################


class GateFilterKeys(Gate):
    def __init__(self, keys: list, id: str = None):
        super().__init__(id=id)
        self._keys = keys

    def __call__(self, x: Sample) -> Sample:
        out = x.copy()
        for k in list(out.keys()):
            if k not in self._keys:
                del out[k]
        return out


class GateCompose(Gate):
    def __init__(self, gates: list, id: str = None):
        super().__init__(id=id)
        self._gates = gates

    def __call__(self, x: Sample) -> Sample:
        out = x
        for g in self._gates:
            out = g(out)
        return out


class UserGateAddKey(Gate):
    def __init__(self, key_to_add: str, value_to_add: int, id: str = None):
        super().__init__(id=id)
        self._key_to_add = key_to_add
        self._value_to_add = value_to_add

    def __call__(self, x: Sample) -> Sample:
        out = x.copy()
        out[self._key_to_add] = self._value_to_add
        return out


###############################################################################
###############################################################################
###############################################################################
###############################################################################


class UserGateSplitValues(Gate1N):
    def __init__(self, value_size: int = 3, id: str = None):
        super().__init__(id=id)
        self._value_size = value_size

    def __call__(self, x: Sample) -> SamplesSequence:

        out = []
        for index in range(self._value_size):
            out_sample = PlainSample()
            for k, v in x.items():
                out_sample[k] = x[k][index]
            out.append(out_sample)
        return SamplesSequence(samples=out)


class UserGateMergeValues(GateN1):
    def __init__(self, id: str = None):
        super().__init__(id=id)

    def __call__(self, x: SamplesSequence) -> Sample:
        out = PlainSample()
        for sample in x:
            for k, v in sample.items():
                if k not in out:
                    out[k] = []
                out[k].append(v)
        return out


s = PlainSample(
    {
        "a": [0, 1, 2],
        "b": [10, 11, 12],
        "c": [100, 101, 102],
        "d": [1000, 1001, 1002],
    }
)


gate_split = UserGateSplitValues()

gate_add_key = UserGateAddKey("e", 10000)
gate_multi = GateNN(
    gates=[
        UserGateAddKey("e", 10000),
        UserGateAddKey("e", 10001),
        UserGateAddKey("e", 10002),
    ]
)
gate_merge = UserGateMergeValues()

for name, parameter in inspect.signature(gate_merge.__call__).parameters.items():
    parameter: inspect.Parameter
    print(name, parameter.annotation)
