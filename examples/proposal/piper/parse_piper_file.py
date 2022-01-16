from choixe.configurations import XConfig
import numpy as np
import rich
from pipelime.pipes.parser import PipeGraph, PipesConfigParser
import matplotlib.pyplot as plt
import networkx as nx
from networkx.drawing.nx_agraph import to_agraph
import random
import string
from faker import Faker


def draw_graph(graph: nx.DiGraph, agraph: bool = True):

    if agraph == False:

        data_indices = []
        op_indices = []
        for index, n in enumerate(graph.nodes):
            n: PipeGraph.PipeGraphNode
            if n.type == "data":
                data_indices.append(n)
            elif n.type == "operation":
                op_indices.append(n)

        nx.draw_networkx_nodes(
            graph,
            pos,
            nodelist=op_indices,
            node_size=800,
            node_shape="s",
            node_color="#ff00aa",
        )
        nx.draw_networkx_nodes(
            graph,
            pos,
            nodelist=data_indices,
            node_size=500,
            node_shape="o",
            node_color="#ff0000",
            label="data",
        )
        nx.draw_networkx_labels(graph, pos, font_size=10, font_family="sans-serif")
        nx.draw_networkx_edges(graph, pos)
        # nx.draw(graph, pos, with_labels=True)
        plt.show()
    else:

        agraph = to_agraph(graph)
        nodes_map = {x.id: x for x in graph.nodes}
        for i, node in enumerate(agraph.iternodes()):
            ref_node: PipeGraph.PipeGraphNode = nodes_map[node]
            if ref_node.type == "operation":
                node.attr["label"] = ref_node.id
                node.attr["style"] = "filled"
                node.attr["fillcolor"] = "#ffd54f"
                node.attr["shape"] = "component"
            elif ref_node.type == "data":
                node.attr["label"] = ref_node.id
                node.attr["style"] = "filled"
                node.attr["fillcolor"] = "#33d54f"
                node.attr["shape"] = "parallelogram"

        agraph.layout("dot")
        fname = "/tmp/ama.png"
        agraph.draw(fname)
        rich.print("Written to", fname)


fake = Faker()

splits_percentages = np.random.uniform(0.01, 0.99, size=(20,))
splits_percentages = np.exp(splits_percentages) / np.sum(np.exp(splits_percentages))
splits_name = [fake.name() for x in splits_percentages]

global_data = {
    "params": {
        "input_folders": [fake.name() for _ in range(10)],
        "converted_folder": "/tmp/converted",
        "summed_folder": "/home/summed",
        "detected_folder": "/home/detected",
        "detection_configuration": "/home/detection_configuration.yml",
        "filtered_folder": "/home/extracted",
        "extraction_configuration": "/home/extraction_configuration.yml",
        "output_folder": "/home/output",
        "checked_folder": "/home/checked",
        "splits": [
            {"name": x, "p": y} for x, y in zip(splits_name, splits_percentages)
        ],
    }
}

# Load configuration
filename = "piper.yml"
cfg = XConfig(filename)

# Create Parser
parser = PipesConfigParser()
parser.set_global_data("params", global_data["params"])
parsed = parser.parse_cfg(cfg.to_dict())
rich.print(parsed)

# Creste graph from parsed configuration
graph: nx.DiGraph = PipeGraph(cfg=parsed)


produced_data = set()

rich.print("execution stack")
rich.print(graph.build_execution_stack())


# Layout graph
pos = nx.nx_pydot.graphviz_layout(graph, prog="dot")

# rich.print("TOPO")
# for n in nx.all_topological_sorts(graph.operations_graph):
#     rich.print(n)

draw_graph(graph)
