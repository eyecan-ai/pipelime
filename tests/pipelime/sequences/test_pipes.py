import pytest
from choixe.spooks import Spook

from pipelime.sequences.operations import (
    OperationFilterByQuery,
    OperationResetIndices,
    OperationSplitByQuery,
    OperationSplits,
    OperationSum,
)
from pipelime.sequences.pipes import NodeGraph, OperationNode, ReaderNode, WriterNode
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.writers.filesystem import UnderfolderWriter
from pipelime.tools.idgenerators import IdGeneratorUUID

# @pytest.fixture
# def simple_graph():
#     return {
#         '__spook__': 'NodeGraph',
#         'options': {
#             'nodes': [
#                 {
#                     '__spook__': 'ReaderNode',
#                     'options': {
#                         'output_data': 'A',
#                         'reader': {
#                             '__spook__': 'UnderfolderReader',
#                             'options': {
#                                 'folder': '/Users/daniele/Downloads/lego_dataset/lego_00',
#                             }
#                         }
#                     }
#                 },
#                 {
#                     '__spook__': 'ReaderNode',
#                     'options': {
#                         'output_data': 'B',
#                         'reader': {
#                             '__spook__': 'UnderfolderReader',
#                             'options': {
#                                 'folder': '/Users/daniele/Downloads/lego_dataset/lego_01'
#                             }
#                         }
#                     }
#                 },
#                 {
#                     '__spook__': 'OperationNode',
#                     'options': {
#                         'input_data': 'A',
#                         'output_data': 'A_filtered',
#                         'operation': {
#                             '__spook__': 'OperationFilterByQuery',
#                             'options': {
#                                 'query': '`metadata.tag` == "image"'
#                             }
#                         }
#                     }
#                 },
#                 {
#                     '__spook__': 'OperationNode',
#                     'options': {
#                         'input_data': ['A_filtered', 'B'],
#                         'output_data': 'C',
#                         'operation': {
#                             '__spook__': 'OperationSum',
#                             'options': {}
#                         }
#                     }
#                 },
#                 {
#                     '__spook__': 'OperationNode',
#                     'options': {
#                         'input_data': 'C',
#                         'output_data': 'D',
#                         'operation': {
#                             '__spook__': 'OperationSplits',
#                             'options': {
#                                 'split_map': {
#                                     'train': 0.45,
#                                     'val': 0.45,
#                                     'test': 0.1
#                                 }
#                             }
#                         }
#                     }
#                 },

#             ]
#         }
#     }


class TestPipes(object):
    def test_simple_graph(self, filesystem_datasets, tmp_path_factory):

        print(filesystem_datasets)
        dataset_folder = filesystem_datasets["minimnist_underfolder"]["folder"]

        out_folders = {k: tmp_path_factory.mktemp(k) for k in ["D0", "D1", "D2"]}
        r = UnderfolderReader(folder=dataset_folder)
        N = len(r)
        EXPECTED = N * 3 - (
            2 * 3
        )  # '`metadatay.half` >= 1.0' query removes 2 samples per input dataset (3)
        generator = IdGeneratorUUID()

        nodes = [
            ReaderNode(
                id=generator.generate(),
                output_data="A",
                reader=UnderfolderReader(folder=dataset_folder),
            ),
            ReaderNode(
                id=generator.generate(),
                output_data="B",
                reader=UnderfolderReader(folder=dataset_folder),
            ),
            ReaderNode(
                id=generator.generate(),
                output_data="C",
                reader=UnderfolderReader(folder=dataset_folder),
            ),
            OperationNode(
                id=generator.generate(),
                input_data=["A", "B", "C"],
                output_data="X",
                operation=OperationSum(),
            ),
            OperationNode(
                id=generator.generate(),
                input_data="X",
                output_data=["X0", "X1"],
                operation=OperationSplitByQuery(query="`metadatay.sample_id` <= 5"),
            ),
            OperationNode(
                id=generator.generate(),
                input_data="X0",
                output_data="X0_f",
                operation=OperationFilterByQuery(query="`metadatay.half` >= 1.0"),
            ),
            OperationNode(
                id=generator.generate(),
                input_data="X1",
                output_data="X1_f",
                operation=OperationFilterByQuery(query="`metadatay.half` >= 1.0"),
            ),
            OperationNode(
                id=generator.generate(),
                input_data=["X0_f", "X1_f"],
                output_data="Y",
                operation=OperationSum(),
            ),
            OperationNode(
                id=generator.generate(),
                input_data="Y",
                output_data="OUT",
                operation=OperationResetIndices(generator=IdGeneratorUUID()),
            ),
            OperationNode(
                id=generator.generate(),
                input_data="OUT",
                output_data={
                    "a": "D0",
                    "b": "D1",
                    "c": "D2",
                },
                operation=OperationSplits(split_map={"a": 0.25, "b": 0.25, "c": 0.50}),
            ),
        ]

        for k, v in out_folders.items():
            nodes.append(
                WriterNode(
                    id=generator.generate(),
                    input_data=k,
                    writer=UnderfolderWriter(
                        folder=out_folders[k],
                        root_files_keys=["cfg", "numbers", "pose"],
                    ),
                )
            )

        # print(n)
        graph = NodeGraph(nodes=nodes)
        import rich

        rich.print(graph.serialize())
        graph = Spook.create(graph.serialize())

        try:
            f, a = graph.draw_to_file()
            print(f, a)
        except ImportError:
            pytest.skip(msg="No pygraphviz module found!")

        graph.execute()

        counter = 0
        for k, v in out_folders.items():
            out_reader = UnderfolderReader(folder=v, copy_root_files=True)
            counter += len(out_reader)
            print("OUT", k, v)

        assert counter == EXPECTED
        print("COUNTER", N, "/", counter)
        print(len(graph._data_cache["X"]))
        print(len(graph._data_cache["X0"]))
        print(len(graph._data_cache["X1"]))
        print(len(graph._data_cache["X0_f"]))
        print(len(graph._data_cache["X1_f"]))
        print(len(graph._data_cache["Y"]))
        print(len(graph._data_cache["D0"]))
        print(len(graph._data_cache["D1"]))
        print(len(graph._data_cache["D2"]))
