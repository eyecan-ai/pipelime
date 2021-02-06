

from pipelime.tools.idgenerators import IdGeneratorInteger, IdGeneratorUUID
import tempfile
from networkx.drawing.nx_agraph import graphviz_layout, to_agraph
from pipelime.factories import Factorizable, GenericFactory
import networkx as nx
from schema import Or, Schema
from abc import abstractmethod
from typing import Hashable, Sequence, Union
from pipelime.sequences.operations import SequenceOperation, SequenceOperationFactory
from pipelime.sequences.samples import SamplesSequence


class PlaceholderDataNode(object):

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return self.name

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return self.name == o.name


class PipeNode(Factorizable):

    def __init__(
            self,
            id: Hashable,
            input_data: Union[str, list, dict],
            output_data: Union[str, list, dict]
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
            raise NotImplementedError(f'{type(v)}')

    def __repr__(self) -> str:
        return str(self.id)


@GenericFactory.register
class OperationNode(PipeNode):

    def __init__(
            self,
            id: Hashable,
            input_data: Union[str, list, dict],
            output_data: Union[str, list, dict],
            operation: SequenceOperation
    ) -> None:

        super().__init__(id=id, input_data=input_data, output_data=output_data)
        self._operation = operation

    # def __repr__(self) -> str:
    #     return str(self.operation.to_dict())

    @property
    def operation(self):
        return self._operation

    @classmethod
    def factory_name(cls) -> str:
        return OperationNode.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.factory_name(),
            'options': {
                'input_data': Or(str, list, dict),
                'output_data': Or(str, list, dict),
                'operation': dict
            }
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        cls.factory_schema().validate(d)
        return OperationNode(
            id=IdGeneratorUUID.generate(),
            input_data=d['options']['input_data'],
            output_data=d['options']['output_data'],
            operation=SequenceOperationFactory.create(d['options']['operation'])
        )

    def to_dict(self) -> dict:
        return {
            'type': self.factory_name(),
            'options': {
                'input_data': self._input_data,
                'output_data': self._output_data,
                'operation': self.operation.to_dict()
            }
        }


@GenericFactory.register
class SourceNode(PipeNode):

    def __init__(
            self,
            id: Hashable,
            output_data: str,
            sequence: SamplesSequence
    ) -> None:

        super().__init__(id=id, input_data=None, output_data=output_data)
        self._sequence = sequence

    @property
    def sequence(self):
        return self._sequence

    @classmethod
    def factory_name(cls) -> str:
        return SourceNode.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.factory_name(),
            'options': {
                'output_data': Or(None, str, list, dict),
                'sequence': dict
            }
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        cls.factory_schema().validate(d)
        return SourceNode(
            id=IdGeneratorUUID.generate(),
            output_data=d['options']['output_data'],
            sequence=GenericFactory.create(d['options']['sequence'])
        )

    def to_dict(self) -> dict:
        return {
            'type': self.factory_name(),
            'options': {
                'input_data': self._input_data,
                'output_data': self._output_data,
                'sequence': self.sequence.to_dict()
            }
        }


@GenericFactory.register
class NodeGraph(Factorizable):

    def __init__(self, nodes: Sequence[PipeNode]):
        self._nodes = nodes
        self._nodes_map = {}
        self._graph = nx.DiGraph()

        for node in self._nodes:
            node: PipeNode
            [self._graph.add_edge(PlaceholderDataNode(x), node) for x in node.input_data(as_list=True)]
            [self._graph.add_edge(node, PlaceholderDataNode(x)) for x in node.output_data(as_list=True)]
            self._nodes_map[node.id] = node

        self._data_cache = {}
        self._sorted_graph = nx.topological_sort(self._graph)

    @classmethod
    def factory_name(cls) -> str:
        return NodeGraph.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.factory_name(),
            'nodes': list
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        cls.factory_schema().validate(d)
        nodes = [GenericFactory.create(c) for c in d['nodes']]
        return NodeGraph(nodes=nodes)

    def to_dict(self) -> dict:
        return {
            'type': self.factory_name(),
            'nodes': [x.to_dict() for x in self._nodes]
        }

    def draw_to_file(self, filename: str = None):
        A = to_agraph(self._graph)
        shapes = ['circle', 'box']
        for i, node in enumerate(A.iternodes()):
            print(i, node)
            if node in self._nodes_map:
                ref = self._nodes_map[node]
                if isinstance(ref, OperationNode):
                    node.attr['label'] = self._nodes_map[node].operation.__class__.__name__
                    node.attr['style'] = 'filled'
                    node.attr['fillcolor'] = 'turquoise'
                    node.attr['shape'] = 'box'
                elif isinstance(ref, SourceNode):
                    node.attr['label'] = self._nodes_map[node].sequence.__class__.__name__
                    node.attr['style'] = 'filled'
                    node.attr['fillcolor'] = '#ff33aa'
                    node.attr['shape'] = 'box'

            else:
                node.attr['shape'] = 'circle'

        A.layout('dot')
        fname = f'{tempfile.NamedTemporaryFile().name}.png' if filename is None else filename
        A.draw(fname)
        return A, fname
