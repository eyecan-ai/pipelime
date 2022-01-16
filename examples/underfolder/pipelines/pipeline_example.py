import os
from pipelime.sequences.writers.filesystem import UnderfolderWriter
from pipelime.sequences.operations import (
    OperationFilterByQuery,
    OperationOrderBy,
    OperationResetIndices,
    OperationSubsample,
    OperationSum,
)
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.tools.idgenerators import IdGeneratorInteger, IdGeneratorUUID
from pipelime.sequences.pipes import NodeGraph, OperationNode, ReaderNode, WriterNode


folder_a = "../../tests/sample_data/datasets/underfolder_minimnist"
folder_b = "../../tests/sample_data/datasets/underfolder_minimnist"

idg = IdGeneratorUUID()
subsampling = 1.0
nodes = [
    ReaderNode(
        id=idg.generate(), output_data="A", reader=UnderfolderReader(folder=folder_a)
    ),
    ReaderNode(
        id=idg.generate(), output_data="B", reader=UnderfolderReader(folder=folder_b)
    ),
    OperationNode(
        id=idg.generate(),
        input_data="A",
        output_data="A_",
        operation=OperationSubsample(factor=subsampling),
    ),
    OperationNode(
        id=idg.generate(),
        input_data="B",
        output_data="B_",
        operation=OperationSubsample(factor=subsampling),
    ),
    OperationNode(
        id=idg.generate(),
        input_data=["A_", "B_"],
        output_data="X",
        operation=OperationSum(),
    ),
    OperationNode(
        id=idg.generate(),
        input_data="X",
        output_data="X_filtered",
        operation=OperationFilterByQuery(query="`metadata.sample_id` >= 5"),
    ),
    OperationNode(
        id=idg.generate(),
        input_data="X_filtered",
        output_data="X_ordered",
        operation=OperationOrderBy(order_keys=["counter"]),
    ),
    OperationNode(
        id=idg.generate(),
        input_data="X_ordered",
        output_data="X_reset",
        operation=OperationResetIndices(generator=IdGeneratorInteger()),
    ),
    WriterNode(
        id=idg.generate(),
        input_data="X_reset",
        writer=UnderfolderWriter(
            folder="/tmp/zzizzino",
            root_files_keys=["camera", "charuco"],
            extensions_map={
                ".*image.*": "jpg",
                ".*pose.*": "txt",
                ".*camera.*|.*charuco.*|.*metadata.*": "yml",
            },
        ),
    ),
]

graph = NodeGraph(nodes=nodes)

graph.execute()

graph.draw_to_file("/tmp/graph.png")
try:
    os.system("open /tmp/graph.png")
except Exception:
    os.system("open /tmp/graph.png")

# graph.execute()
