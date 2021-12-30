from pipelime.sequences.readers.filesystem import UnderfolderReader
import rich


base_reader = UnderfolderReader(folder="datasets/A")
for sample in base_reader:
    rich.print("Sample:")
    for key in sorted(sample.keys()):
        rich.print("\t", key, ":", sample[key])


###############################################################################
###############################################################################
## CODE TO GENERATE THE DATASETS SUBFOLDER
###############################################################################
###############################################################################
# from pipelime.sequences.samples import PlainSample, SamplesSequence
# from pipelime.sequences.writers.filesystem import UnderfolderWriter
# import networkx as nx
# from pathlib import Path
# dataset_folder = Path("datasets")
# keys = ["A", "B", "C", "D", "E", "F"]
# subfolders = {x: dataset_folder / x for x in keys}

# for key in keys:
#     UnderfolderWriter(folder=subfolders[key])(
#         SamplesSequence([PlainSample({key: i}, id=i) for i in range(2)])
#     )

# g = nx.full_rary_tree(3, len(keys))
# for u, v, a in g.edges(data=True):
#     key_source = keys[u]
#     key_target = keys[v]
#     folder_source = str(subfolders[key_source])
#     folder_target = str(subfolders[key_target])
#     UnderfolderReader.link_underfolders(folder_source, folder_target)
#     print(u, v, keys[u], keys[v])
###############################################################################
###############################################################################
