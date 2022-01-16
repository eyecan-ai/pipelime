from abc import ABC, abstractmethod
from typing import Any, Callable, Hashable, Sequence, Union
import collections
import networkx as nx
from pipelime import sequences
from pipelime.sequences.samples import PlainSample, Sample, SamplesSequence
import matplotlib.pyplot as plt
import uuid
import rich
import time
import numpy as np
import copy


class NodeGraph:
    ACTIVE_GRAPH = None

    def __init__(self, name: str) -> None:
        self._name = name
        self._graph = nx.DiGraph()

    @property
    def graph(self) -> nx.DiGraph:
        return self._graph

    def add_node(self, node: "GNode") -> None:
        self._graph.add_node(node)

    def add_edge(self, node_from: "GNode", node_to: "GNode") -> None:
        self._graph.add_edge(node_to, node_from)
        rich.print("Connecting", node_from, node_to)

    def __enter__(self):
        print("ACTIVE GRAPH", self)
        NodeGraph.ACTIVE_GRAPH = self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("DEACTIVE GRAPH", None)
        NodeGraph.ACTIVE_GRAPH = None


class GNode:
    def __init__(self) -> None:
        self._graph = NodeGraph.ACTIVE_GRAPH
        assert self._graph is not None
        self._predecessor = None

    @property
    def predecessor(self) -> "GNode":
        return self._predecessor

    def __eq__(self, comp: "GNode") -> bool:
        return hash(self) == hash(comp)

    def __ne__(self, comp: "GNode") -> bool:
        return not self.__eq__(comp)

    def __hash__(self):
        return id(self)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({hash(self)})"

    def __rshift__(self, other):
        active_graph = NodeGraph.ACTIVE_GRAPH
        if active_graph is not None:
            node_from = GNode(active_graph, self)
            node_to = GNode(active_graph, other)
            active_graph.add_edge(node_from, node_to)

    @classmethod
    def build_node_sequence(cls, tail: "GNode"):
        pointer = tail
        sequence = [pointer]
        while pointer.predecessor is not None:
            pointer = pointer.predecessor
            sequence.append(pointer)
            if not isinstance(pointer, GNode):
                break
        sequence = list(reversed(sequence))
        return sequence[0], sequence[1:]


class Gate01(GNode):
    def __init__(self) -> None:
        super().__init__()

    def __call__(self, x: Sample) -> GNode:
        self._predecessor = x
        return self

    def apply(self, x: Sample) -> Sample:
        return x


class Gate11(GNode):
    def __init__(self) -> None:
        super().__init__()

    def __call__(
        self, x: Union[GNode, Sequence[GNode]]
    ) -> Union[GNode, Sequence[GNode]]:
        if isinstance(x, GNode):
            self._predecessor = x
            return self
        elif isinstance(x, collections.Sequence):
            out_sequence = []
            for node in x:
                out_sequence.append(copy.copy(self)(node))
            return out_sequence

    @abstractmethod
    def apply(self, x: Sample) -> Sample:
        raise NotImplementedError()


class GateIn(GNode):
    def __init__(self) -> None:
        super().__init__()

    def __call__(self, sequence: SamplesSequence) -> Sequence[Gate01]:
        out = []
        for sample in sequence:
            out.append(Gate01()(sample))
        return out


class GateFilterKey(Gate11):
    def __init__(self, keys: Sequence[str]) -> None:
        super().__init__()
        self._keys = keys

    def apply(self, sample: Sample) -> Sample:
        out = sample.copy()
        for key in list(out.keys()):
            if key not in self._keys:
                del out[key]
        return out


class GateAddKey(Gate11):
    def __init__(self, key: str, value: int) -> None:
        super().__init__()
        self._key = key
        self._value = value

    def apply(self, sample: Sample) -> Sample:
        out = sample.copy()
        out[self._key] = self._value
        return out


class GateAddKey(Gate11):
    def __init__(self, key: str, value: int) -> None:
        super().__init__()
        self._key = key
        self._value = value

    def apply(self, sample: Sample) -> Sample:
        out = sample.copy()
        out[self._key] = self._value
        return out


samples = []
for i in range(10):
    samples.append(
        PlainSample(
            {
                "sample_id": i,
                "image": np.random.uniform(0, 1, (128, 128, 3)),
                "labels": [
                    {
                        "type": "box",
                        "roi": np.random.randint(0, 500, (4,)).tolist(),
                        "class_id": 0,
                    },
                    {
                        "type": "box",
                        "roi": np.random.randint(0, 500, (4,)).tolist(),
                        "class_id": 1,
                    },
                    {
                        "type": "box",
                        "roi": np.random.randint(0, 500, (4,)).tolist(),
                        "class_id": 2,
                    },
                    {
                        "type": "box",
                        "roi": np.random.randint(0, 500, (4,)).tolist(),
                        "class_id": 3,
                    },
                ],
            }
        )
    )

sequence = SamplesSequence(samples)

graph = NodeGraph("graph")


with graph:

    g_in = GateIn()
    g_f0 = GateFilterKey(["sample_id", "image"])
    g_f1 = GateFilterKey(["sample_id"])
    g_f2 = GateAddKey("pino", 11)

    x = g_in(sequence)
    x = g_f0(x)
    x = g_f1(x)
    x = g_f2(x)


for out in x:
    out: GNode

    p, nodes = out.build_node_sequence(out)
    for n in nodes:
        p = n.apply(p)

    rich.print(p)
