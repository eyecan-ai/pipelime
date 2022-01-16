from choixe.configurations import XConfig
import rich
from pipelime.pipes.parser import PipeGraph, PipesConfigParser
import matplotlib.pyplot as plt
import networkx as nx
from networkx.drawing.nx_agraph import to_agraph

global_data = {
    "params": {
        "input_folders": ["a", "b", "c"],
        "converted_folder": "/tmp/converted",
        "summed_folder": "/home/summed",
        "detected_folder": "/home/detected",
        "detection_configuration": "/home/detection_configuration.yml",
        "extracted_folder": "/home/extracted",
        "extraction_configuration": "/home/extraction_configuration.yml",
    }
}

filename = "piper.yml"
cfg = XConfig(filename)

parser = PipesConfigParser()
parser.set_global_data("params", global_data["params"])
parsed = parser.parse_cfg(cfg.to_dict())
rich.print(parsed)

rich.print("GRAPh")

graph: nx.DiGraph = PipeGraph.build_nodes_graph(parsed)
pos = nx.nx_pydot.graphviz_layout(graph, prog="dot")

agraph = True
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
        print(ref_node.name, ref_node.type)
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
