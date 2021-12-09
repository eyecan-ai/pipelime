import tempfile
from typing import Hashable, Sequence, Union

import networkx as nx
from choixe.spooks import Spook
from schema import Or

from pipelime.sequences.operations import SequenceOperation
from pipelime.sequences.readers.base import BaseReader
from pipelime.sequences.samples import SamplesSequence
from pipelime.sequences.writers.base import BaseWriter
from pipelime.tools.idgenerators import IdGeneratorUUID


class PlaceholderDataNode(object):
    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return self.name

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return self.name == o.name


class PipeNode(object):
    def __init__(
        self,
        id: Hashable,
        input_data: Union[str, list, dict],
        output_data: Union[str, list, dict],
    ) -> None:

        self._id = id
        self._input_data = input_data
        self._output_data = output_data

    @property
    def id(self):
        return self._id

    def input_data(self, as_list: bool = False):
        if as_list:
            return self._data_as_list(self._input_data)
        return self._input_data

    def output_data(self, as_list: bool = False):
        if as_list:
            return self._data_as_list(self._output_data)
        return self._output_data

    def _data_as_list(self, v: any):
        if v is None:
            return []
        elif isinstance(v, str):
            return [v]
        elif isinstance(v, list):
            return v
        elif isinstance(v, dict):
            return list(v.values())
        else:
            raise NotImplementedError(f"{type(v)}")

    def __repr__(self) -> str:
        return str(self.id)


class OperationNode(PipeNode, Spook):
    def __init__(
        self,
        id: Hashable,
        input_data: Union[str, list, dict],
        output_data: Union[str, list, dict],
        operation: SequenceOperation,
    ) -> None:

        super().__init__(id=id, input_data=input_data, output_data=output_data)
        self._operation = operation

    @property
    def operation(self):
        return self._operation

    @classmethod
    def bean_schema(cls) -> dict:
        return {
            "input_data": Or(str, list, dict),
            "output_data": Or(str, list, dict),
            "operation": dict,
        }

    @classmethod
    def from_dict(cls, d: dict):
        return OperationNode(
            id=IdGeneratorUUID().generate(),
            input_data=d["input_data"],
            output_data=d["output_data"],
            operation=Spook.create(d["operation"]),
        )

    def to_dict(self):
        return {
            "input_data": self._input_data,
            "output_data": self._output_data,
            "operation": self.operation.serialize(),
        }


class ReaderNode(PipeNode, Spook):
    def __init__(self, id: Hashable, output_data: str, reader: BaseReader) -> None:

        super().__init__(id=id, input_data=None, output_data=output_data)
        self._reader = reader

    @property
    def reader(self):
        return self._reader

    @classmethod
    def bean_schema(cls) -> dict:
        return {"output_data": Or(None, str, list, dict), "reader": dict}

    @classmethod
    def from_dict(cls, d: dict):
        return ReaderNode(
            id=IdGeneratorUUID().generate(),
            output_data=d["output_data"],
            reader=Spook.create(d["reader"]),
        )

    def to_dict(self):
        return {"output_data": self._output_data, "reader": self.reader.serialize()}


class WriterNode(PipeNode, Spook):
    def __init__(self, id: Hashable, input_data: str, writer: BaseWriter) -> None:

        super().__init__(id=id, output_data=None, input_data=input_data)
        self._writer = writer

    @property
    def writer(self):
        return self._writer

    @classmethod
    def bean_schema(cls) -> dict:
        return {"input_data": Or(None, str, list, dict), "writer": dict}

    @classmethod
    def from_dict(cls, d: dict):
        return WriterNode(
            id=IdGeneratorUUID().generate(),
            input_data=d["input_data"],
            writer=Spook.create(d["writer"]),
        )

    def to_dict(self):
        return {"input_data": self._input_data, "writer": self.writer.serialize()}


class NodeGraph(Spook):
    def __init__(self, nodes: Sequence[PipeNode]):
        self._nodes = nodes
        self._nodes_map = {}
        self._graph = nx.DiGraph()

        for node in self._nodes:
            node: PipeNode
            [
                self._graph.add_edge(PlaceholderDataNode(x), node)
                for x in node.input_data(as_list=True)
            ]
            [
                self._graph.add_edge(node, PlaceholderDataNode(x))
                for x in node.output_data(as_list=True)
            ]
            self._nodes_map[node.id] = node

        self._data_cache = {}
        self._sorted_graph = nx.topological_sort(self._graph)

    def clear(self):
        self._data_cache.clear()

    def execute(self):
        self.clear()
        for node in self._sorted_graph:
            if isinstance(node, ReaderNode):
                for data in node.output_data(as_list=True):
                    self._data_cache[data] = node.reader

            if isinstance(node, OperationNode):
                data = self._cache_to_node(node, self._data_cache)
                out_data = node.operation(data)
                self._node_to_cache(out_data, node, self._data_cache)

            if isinstance(node, WriterNode):
                data = self._cache_to_node(node, self._data_cache)
                node.writer(data)

    def _cache_to_node(
        self, node: PipeNode, cache: dict
    ) -> Union[SamplesSequence, list, dict]:
        v = node.input_data(as_list=False)
        if v is None:
            return None
        elif isinstance(v, str):
            return cache[v]
        elif isinstance(v, list):
            return [cache[x] for x in v]
        elif isinstance(v, dict):
            return {k: cache[v] for k, v in v.items()}
        else:
            raise NotImplementedError(f"{type(v)}")

    def _node_to_cache(
        self, data: Union[SamplesSequence, list, dict], node: PipeNode, cache: dict
    ):
        v = node.output_data(as_list=False)
        if v is None:
            pass
        elif isinstance(v, str):
            cache[v] = data
        elif isinstance(v, list):
            for idx, name in enumerate(v):
                cache[name] = data[idx]
        elif isinstance(v, dict):
            for k, name in v.items():
                cache[name] = data[k]
        else:
            raise NotImplementedError(f"{type(v)}")

    @classmethod
    def bean_schema(cls) -> dict:
        return {"nodes": list}

    @classmethod
    def from_dict(cls, d: dict):
        nodes = [Spook.create(c) for c in d["nodes"]]
        return NodeGraph(nodes=nodes)

    def to_dict(self):
        return {"nodes": [x.serialize() for x in self._nodes]}

    def draw_to_file(self, filename: str = None):

        try:
            from networkx.drawing.nx_agraph import to_agraph

            A = to_agraph(self._graph)
            for i, node in enumerate(A.iternodes()):
                print(i, node)
                if node in self._nodes_map:
                    ref = self._nodes_map[node]
                    if isinstance(ref, OperationNode):
                        node.attr["label"] = self._nodes_map[
                            node
                        ].operation.__class__.__name__
                        node.attr["style"] = "filled"
                        node.attr["fillcolor"] = "#ffd54f"
                        node.attr["shape"] = "box"
                    elif isinstance(ref, ReaderNode):
                        node.attr["label"] = self._nodes_map[
                            node
                        ].reader.__class__.__name__
                        node.attr["style"] = "filled"
                        node.attr["fillcolor"] = "#009688"
                        node.attr["shape"] = "box"
                    elif isinstance(ref, WriterNode):
                        node.attr["label"] = self._nodes_map[
                            node
                        ].writer.__class__.__name__
                        node.attr["style"] = "filled"
                        node.attr["fillcolor"] = "#9c27b0"
                        node.attr["shape"] = "box"

                else:
                    node.attr["shape"] = "circle"

            A.layout("dot")
            fname = (
                f"{tempfile.NamedTemporaryFile().name}.png"
                if filename is None
                else filename
            )
            A.draw(fname)
            return A, fname

        except ImportError as e:
            print(e)
            return None, None
