from pipelime.sequences.readers.filesystem import (
    UnderfolderLinksPlugin,
    UnderfolderReader,
)
import networkx as nx
import matplotlib.pyplot as plt
from rich.table import Table
from rich.console import Console


###############################################################################
###############################################################################
## CODE TO re-GENERATE THE DATASETS SUBFOLDER
###############################################################################
###############################################################################
# from pipelime.sequences.readers.filesystem import UnderfolderLinksPlugin
# from pipelime.sequences.samples import PlainSample, SamplesSequence
# from pipelime.sequences.writers.filesystem import UnderfolderWriter
# import networkx as nx
# from pathlib import Path

# dataset_folder = Path("datasets")
# keys = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"]
# subfolders = {x: dataset_folder / x for x in keys}

# for key in keys:
#     UnderfolderWriter(folder=subfolders[key], extensions_map={key: "yml"})(
#         SamplesSequence([PlainSample({key: {"data": i}}, id=i) for i in range(2)])
#     )

# g = nx.full_rary_tree(3, len(keys))
# for u, v, a in g.edges(data=True):
#     key_source = keys[u]
#     key_target = keys[v]
#     folder_source = str(subfolders[key_source])
#     folder_target = str(subfolders[key_target])
#     UnderfolderLinksPlugin.link_underfolders(folder_source, folder_target)
#     print(u, v, keys[u], keys[v])
###############################################################################
###############################################################################


table = Table(show_header=True, header_style="bold magenta")
base_reader = UnderfolderReader(folder="datasets/A")
for sample in base_reader:
    table.add_row("Sample", sample.id, "")
    for key in sorted(sample.keys()):
        table.add_row("", key, str(sample[key]))

Console().print(table)

## Load plugins and show Links Graph if any
for plugin_name, plugin in base_reader.plugins_map.items():
    if isinstance(plugin, UnderfolderLinksPlugin):
        nx.draw(plugin.links_graph, with_labels=True)
        plt.show()
