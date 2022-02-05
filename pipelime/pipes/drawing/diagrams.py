import numpy as np
from pipelime.pipes.drawing.base import NodesGraphDrawer
from pipelime.pipes.graph import (
    GraphNode,
    GraphNodeData,
    GraphNodeOperation,
    DAGNodesGraph,
)
from tempfile import NamedTemporaryFile
import imageio
from diagrams.custom import Custom
import pathlib


def get_asset_path(asset_name) -> str:
    return str(pathlib.Path(__file__).parent / "assets" / asset_name)


class DataNode(Custom):
    def __init__(self, label):
        pathlib.Path(__file__).parent.resolve()
        super().__init__(
            label,
            get_asset_path("NodeUnderfolder.png"),
        )


class DiagramsNodesGraphDrawer(NodesGraphDrawer):
    def draw(self, graph: DAGNodesGraph) -> np.ndarray:
        """Draw graph using Diagrams library

        Args:
            graph (NodesGraph): [description]

        Returns:
            np.ndarray: [description]
        """

        from diagrams import Diagram, Cluster
        from diagrams.programming.language import Python as OperationNode

        f = NamedTemporaryFile(suffix="")

        with Diagram(
            "Pipeline", show=False, direction="TB", filename=f.name, outformat="png"
        ):

            execution_stack = graph.build_execution_stack()

            nodes_map = {}

            for layer_index, layer in enumerate(execution_stack):
                with Cluster("Layer {}".format(layer_index)):
                    for node in layer:
                        node: GraphNode
                        if isinstance(node, GraphNodeOperation):
                            nodes_map[node.id] = OperationNode(node.name)
                        elif isinstance(node, GraphNodeData):
                            nodes_map[node.id] = DataNode(node.name)

            for node in graph.raw_graph.nodes():
                node: GraphNode
                if isinstance(node, GraphNodeOperation):
                    pass
                elif isinstance(node, GraphNodeData):
                    nodes_map[node.id] = DataNode(node.name)

            for edge in graph.raw_graph.edges():
                n0: GraphNode = edge[0]
                n1: GraphNode = edge[1]
                nodes_map[n0.id] >> nodes_map[n1.id]

        try:
            f.close()
        except:
            pass
        img = imageio.imread(f"{f.name}.png")
        return img
