from typing import Sequence, Set
from pipelime.pipes.model import NodeModel, DAGModel
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


class DAGNodesGraph:
    class GraphAttrs:
        INPUT_PORT = "input_port"
        OUTPUT_PORT = "output_port"
        EDGE_TYPE = "edge_type"

    def __init__(self, raw_graph: nx.DiGraph):
        """Initialize the DAG graph starting from a raw directed graph (networkx).

        Args:
            raw_graph (nx.DiGraph): The raw graph to be converted to a DAG graph.
        """
        super().__init__()
        self._raw_graph = raw_graph

    @property
    def raw_graph(self) -> nx.DiGraph:
        """The raw graph (networkx) of the DAG.

        Returns:
            nx.DiGraph: The raw graph (networkx) of the DAG.
        """
        return self._raw_graph

    @property
    def operations_graph(self) -> "DAGNodesGraph":
        """The operations graph (networkx) of the DAG. It is a filtered version of the
        raw graph with operations nodes only.

        Returns:
            DAGNodesGraph: The operations graph (networkx) of the DAG.
        """
        return DAGNodesGraph.filter_node_graph(
            self, [GraphNode.GRAPH_NODE_TYPE_OPERATION]
        )

    @property
    def data_graph(self) -> "DAGNodesGraph":
        """The data graph (networkx) of the DAG. It is a filtered version of the
        raw graph with data nodes only.

        Returns:
            DAGNodesGraph: The data graph (networkx) of the DAG.
        """
        return DAGNodesGraph.filter_node_graph(self, [GraphNode.GRAPH_NODE_TYPE_DATA])

    @property
    def root_nodes(self) -> Sequence[GraphNode]:
        """The root nodes of the DAG. They are the nodes that have no predecessors.

        Returns:
            Sequence[GraphNode]: The root nodes of the DAG.
        """
        return [
            node
            for node in self.raw_graph.nodes
            if len(list(self.raw_graph.predecessors(node))) == 0
        ]

    def consumable_operations(
        self,
        produced_data: Set[GraphNodeData],
    ) -> Set[GraphNodeOperation]:
        """Given a set of produced data, returns the set of operations that can be
        consumed given the produced data, i.e. the operations that have inputs available

        Args:
            produced_data (Set[GraphNodeData]): The set of produced data.

        Returns:
            Set[GraphNodeOperation]: The set of operations that can be consumed given
            the produced data.
        """

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
        """Given a set of operations, consume them and return the set of produced data,
        i.e. the data are the outputs of the operations.

        Args:
            operation_nodes (Sequence[GraphNodeOperation]): The set of operations to be
            consumed.

        Returns:
            Set[GraphNodeData]: The set of produced data.
        """
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
        """Builds the execution stack of the DAG. The execution stack is a list of lists
        of operations. Each list of operations is a virtual layer of operations that
        can be executed in parallel because they have no dependencies.

        Returns:
            Sequence[Sequence[GraphNodeOperation]]: The execution stack of the DAG.
        """

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
        dag_model: DAGModel,
    ) -> "DAGNodesGraph":
        """Builds the nodes graph of the DAG starting from a plain DAG model.

        Returns:
            DAGNodesGraph: The nodes graph of the DAG.
        """

        g = nx.DiGraph()

        for node_name, node in dag_model.nodes.items():
            node: NodeModel

            inputs = node.inputs
            outputs = node.outputs

            if inputs is not None:
                for input_name, input_value in inputs.items():
                    if isinstance(input_value, str):
                        input_value = [input_value]

                    attrs = {}
                    for x in input_value:
                        n0 = GraphNodeData(str(x), str(x))
                        n1 = GraphNodeOperation(node_name, node)
                        g.add_edge(n0, n1)
                        attrs.update(
                            {
                                (n0, n1): {
                                    DAGNodesGraph.GraphAttrs.INPUT_PORT: input_name,
                                    DAGNodesGraph.GraphAttrs.EDGE_TYPE: "DATA_2_OPERATION",
                                }
                            }
                        )
                    nx.set_edge_attributes(g, attrs)

            if outputs is not None:
                for output_name, output_value in outputs.items():
                    if isinstance(output_value, str):
                        output_value = [output_value]

                    attrs = {}
                    for x in output_value:
                        n0 = GraphNodeOperation(node_name, node)
                        n1 = GraphNodeData(str(x), str(x))
                        g.add_edge(n0, n1)
                        attrs.update(
                            {
                                (n0, n1): {
                                    DAGNodesGraph.GraphAttrs.OUTPUT_PORT: output_name,
                                    DAGNodesGraph.GraphAttrs.EDGE_TYPE: "OPERATION_2_DATA",
                                }
                            }
                        )

                    nx.set_edge_attributes(g, attrs)

        return DAGNodesGraph(raw_graph=g)

    @classmethod
    def filter_raw_graph(
        cls,
        full_graph: nx.DiGraph,
        types: Sequence[str],
    ) -> nx.DiGraph:
        """Filters the raw graph of the DAG to keep only the nodes of the given types.

        Args:
            full_graph (nx.DiGraph): The raw graph of the DAG.
            types (Sequence[str]): The types of the nodes to keep.

        Returns:
            nx.DiGraph: The filtered raw graph of the DAG.
        """

        filtered_graph: nx.DiGraph = nx.DiGraph()

        for node in full_graph.nodes:
            if node.type not in types:
                predecessors = list(full_graph.predecessors(node))
                successors = list(full_graph.successors(node))
                pairs = list(itertools.product(predecessors, successors))
                for pair in pairs:
                    filtered_graph.add_edge(pair[0], pair[1])

        # There could be single layers graph without connections, all nodes have to be kept
        # and are therefore added to the graph
        for node in full_graph.nodes:
            if node.type in types:
                filtered_graph.add_node(node)

        return filtered_graph

    @classmethod
    def filter_node_graph(
        cls,
        nodes_graph: "DAGNodesGraph",
        types: Sequence[str],
    ) -> "DAGNodesGraph":
        """Filters the nodes graph of the DAG to keep only the nodes of the given types.
        It wraps the raw graph into a DAGNodesGraph.

        Args:
            nodes_graph (DAGNodesGraph): The nodes graph of the DAG.
            types (Sequence[str]): The types of the nodes to keep.

        Returns:
            DAGNodesGraph: The filtered nodes graph of the DAG.
        """
        return DAGNodesGraph(
            raw_graph=cls.filter_raw_graph(nodes_graph.raw_graph, types)
        )
