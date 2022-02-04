from typing import Sequence, Set
from pipelime.pipes.model import NodeModel, NodesModel
import networkx as nx
import itertools


class GraphNode:
    GRAPH_NODE_TYPE_OPERATION = "operation"
    GRAPH_NODE_TYPE_DATA = "data"

    def __init__(self, name: str, type: str):
        self.name = name
        self.type = type

    @property
    def id(self):
        return f"{self.type}({self.name})"

    def __hash__(self) -> int:
        return hash(f"{self.type}({self.name})")

    def __repr__(self) -> str:
        return f"{self.type}({self.name})"

    def __eq__(self, o: object) -> bool:
        return self.id == o.id


class GraphNodeOperation(GraphNode):
    def __init__(self, name: str, node_model: NodeModel):
        super().__init__(name, GraphNode.GRAPH_NODE_TYPE_OPERATION)
        self._node_model = node_model

    @property
    def node_model(self) -> NodeModel:
        return self._node_model


class GraphNodeData(GraphNode):
    def __init__(self, name: str, path: str):
        super().__init__(name, GraphNode.GRAPH_NODE_TYPE_DATA)
        self._path = path

    @property
    def path(self) -> str:
        return self._path


class NodesGraph:
    def __init__(self, raw_graph: nx.DiGraph):
        super().__init__()
        self._raw_graph = raw_graph

    @property
    def raw_graph(self) -> nx.DiGraph:
        return self._raw_graph

    @property
    def operations_graph(self):
        return NodesGraph.filter_node_graph(self, [GraphNode.GRAPH_NODE_TYPE_OPERATION])

    @property
    def data_graph(self):
        return NodesGraph.filter_node_graph(self, [GraphNode.GRAPH_NODE_TYPE_DATA])

    @property
    def root_nodes(self) -> Sequence[GraphNode]:
        return [
            node
            for node in self.raw_graph.nodes
            if len(list(self.raw_graph.predecessors(node))) == 0
        ]

    def consumable_operations(
        self, produced_data: Set[GraphNodeData]
    ) -> Set[GraphNodeOperation]:

        consumables = set()
        for node in self.operations_graph.raw_graph.nodes:
            in_data = [
                x
                for x in self.raw_graph.predecessors(node)
                if isinstance(x, GraphNodeData)
            ]
            if all(x in produced_data for x in in_data):
                consumables.add(node)
        return consumables

    def consume(
        self,
        operation_nodes: Sequence[GraphNodeOperation],
    ) -> Set[GraphNodeData]:
        consumed_data = set()
        for node in operation_nodes:
            out_data = [
                x
                for x in self.raw_graph.successors(node)
                if isinstance(x, GraphNodeData)
            ]
            consumed_data.update(out_data)
        return consumed_data

    def build_execution_stack(self) -> Sequence[Sequence[GraphNodeOperation]]:

        # the execution stack is a list of lists of nodes. Each list represents a
        execution_stack: Sequence[Sequence[GraphNodeOperation]] = []

        # initalize global produced data with the root nodes
        global_produced_data = set()
        global_produced_data.update(
            [x for x in self.root_nodes if isinstance(x, GraphNodeData)]
        )

        # set of operations that have been consumed
        consumed_operations = set()

        while True:
            # which operations can be consumed given the produced data?
            consumable: set = self.consumable_operations(global_produced_data)

            # Remove from consumable operations the ones that have already been consumed
            consumable = consumable.difference(consumed_operations)

            # No consumable operations? We are done!
            if len(consumable) == 0:
                break

            # If not empty, append consumable operations to the execution stack
            execution_stack.append(consumable)

            # The set of produced data is the union of all the consumed operations
            produced_data: set = self.consume(consumable)

            # Add the consumed operations to the consumed operations set
            consumed_operations.update(consumable)

            # Add the produced data to the global produced data
            global_produced_data.update(produced_data)

        return execution_stack

    @classmethod
    def build_nodes_graph(
        cls,
        nodes_model: NodesModel,
    ) -> "NodesGraph":

        g = nx.DiGraph()

        for node_name, node in nodes_model.nodes.items():
            node: NodeModel

            inputs = node.inputs
            outputs = node.outputs

            for _, input_value in inputs.items():
                if isinstance(input_value, str):
                    input_value = [input_value]
                [
                    g.add_edge(
                        GraphNodeData(str(x), str(x)),
                        GraphNodeOperation(node_name, node),
                    )
                    for x in input_value
                ]

            for _, output_value in outputs.items():
                if isinstance(output_value, str):
                    output_value = [output_value]
                [
                    g.add_edge(
                        GraphNodeOperation(node_name, node),
                        GraphNodeData(str(x), str(x)),
                    )
                    for x in output_value
                ]

        return NodesGraph(raw_graph=g)

    @classmethod
    def filter_raw_graph(
        cls, full_graph: nx.DiGraph, types: Sequence[str]
    ) -> nx.DiGraph:

        filtered_graph: nx.DiGraph = nx.DiGraph()

        for node in full_graph.nodes:
            if node.type not in types:
                predecessors = list(full_graph.predecessors(node))
                successors = list(full_graph.successors(node))
                pairs = list(itertools.product(predecessors, successors))
                for pair in pairs:
                    filtered_graph.add_edge(pair[0], pair[1])

        return filtered_graph

    @classmethod
    def filter_node_graph(
        cls, nodes_graph: "NodesGraph", types: Sequence[str]
    ) -> nx.DiGraph:
        return NodesGraph(raw_graph=cls.filter_raw_graph(nodes_graph.raw_graph, types))
