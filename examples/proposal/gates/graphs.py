from abc import ABC, abstractmethod
from typing import Any, Callable, Sequence
import networkx as nx
from pipelime import sequences
from pipelime.sequences.samples import PlainSample, Sample, SamplesSequence
import matplotlib.pyplot as plt
import uuid
import rich
import time


class NodeGraph:
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


class GNode:
    def __init__(
        self,
        graph: NodeGraph,
        callable: Callable = None,
        id: str = None,
    ) -> None:
        self._graph = graph
        self._id = id if id is not None else str(uuid.uuid1())
        self._callable: Callable = callable

    @property
    def id(self) -> str:
        return self._id

    @property
    def callable(self) -> Callable:
        return self._callable

    @callable.setter
    def callable(self, callable: Callable) -> None:
        self._callable = callable

    def __eq__(self, comp: "GNode") -> bool:
        return self.id == comp.id

    def __ne__(self, comp: "GNode") -> bool:
        return not self.__eq__(comp)

    def __hash__(self):
        return hash(self.id)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.id})"


class Gate(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def __call__(self, x: Sample) -> Sample:
        raise NotImplementedError()


class CustomGate(Gate):
    def __init__(self, key: str, multiplier: float) -> None:
        super().__init__()
        self._key = key
        self._multiplier = multiplier

    def __call__(self, x: Sample) -> Sample:
        out = x.copy()
        out[self._key] *= self._multiplier
        return out


class SequencePointer:
    def __init__(self, input_sequence: Sequence[Sample], index: int) -> None:
        self._input_sequence = input_sequence
        self._index = index

    def __call__(self) -> Sample:
        return self._input_sequence[self._index]


samples = []
for i in range(10):
    samples.append(PlainSample({"a": i, "b": i, "c": i, "d": i, "e": i, "f": i}))
sequence = SamplesSequence(samples)


gates = [
    SequencePointer(sequence, 9),
    CustomGate("a", 2),
    CustomGate("b", 2),
    CustomGate("c", 2),
    CustomGate("d", 2),
    CustomGate("e", 2),
    CustomGate("f", 2),
]

graph = NodeGraph("graph")
last_node = None
for gate_index in range(0, len(gates) - 1, 1):
    node_start = (
        GNode(graph, gates[gate_index], id=f"N_{gate_index}")
        if last_node is None
        else last_node
    )
    node_end = GNode(graph, gates[gate_index + 1], id=f"Gate_{gate_index+1}")
    last_node = node_end
    graph.add_edge(node_start, node_end)


pointer: GNode = last_node
callable_sequence = []
while True:
    callable_sequence.append(pointer.callable)
    successors = list(graph.graph.successors(pointer))
    if len(successors) == 0:
        break
    else:
        pointer = successors[0]

callable_sequence = list(reversed(callable_sequence))


for i in range(1000):
    t1 = time.time()
    p = callable_sequence[0]()
    for callable_index in range(1, len(callable_sequence)):
        callable = callable_sequence[callable_index]
        p = callable(p)

    t2 = time.time()
    rich.print(f"Time: {t2 - t1}")

print("Out", p)

# rich.print(sequence)
# nx.draw(graph.graph, with_labels=True)
# plt.show()
