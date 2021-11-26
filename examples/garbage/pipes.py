from choixe.spooks import Spook
from pipelime.sequences.pipes import NodeGraph

graph = {
    "__spook__": "NodeGraph",
    "options": {
        "nodes": [
            {
                "__spook__": "ReaderNode",
                "options": {
                    "output_data": "A",
                    "reader": {
                        "__spook__": "UnderfolderReader",
                        "options": {
                            "folder": "/Users/daniele/Downloads/lego_dataset/lego_00",
                        },
                    },
                },
            },
            {
                "__spook__": "ReaderNode",
                "options": {
                    "output_data": "B",
                    "reader": {
                        "__spook__": "UnderfolderReader",
                        "options": {
                            "folder": "/Users/daniele/Downloads/lego_dataset/lego_01"
                        },
                    },
                },
            },
            {
                "__spook__": "OperationNode",
                "options": {
                    "input_data": "A",
                    "output_data": "A1",
                    "operation": {
                        "__spook__": "OperationFilterByQuery",
                        "options": {"query": '`metadata.tag` == "black"'},
                    },
                },
            },
            {
                "__spook__": "OperationNode",
                "options": {
                    "input_data": ["A1", "B"],
                    "output_data": "C",
                    "operation": {"__spook__": "OperationSum", "options": {}},
                },
            },
            {
                "__spook__": "OperationNode",
                "options": {
                    "input_data": "C",
                    "output_data": "D",
                    "operation": {
                        "__spook__": "OperationFilterByQuery",
                        "options": {"query": '`metadata.tag` == "black"'},
                    },
                },
            },
            {
                "__spook__": "OperationNode",
                "options": {
                    "input_data": "D",
                    "output_data": "D1",
                    "operation": {
                        "__spook__": "OperationSubsample",
                        "options": {"factor": 0.1},
                    },
                },
            },
            {
                "__spook__": "OperationNode",
                "options": {
                    "input_data": "D1",
                    "output_data": {"a": "train", "b": "test", "c": "val"},
                    "operation": {
                        "__spook__": "OperationSplits",
                        "options": {"split_map": {"a": 0.45, "b": 0.45, "c": 0.1}},
                    },
                },
            },
            {
                "__spook__": "WriterNode",
                "options": {
                    "input_data": "train",
                    "writer": {
                        "__spook__": "UnderfolderWriter",
                        "options": {"folder": "/tmp/zizzino/train"},
                    },
                },
            },
            {
                "__spook__": "WriterNode",
                "options": {
                    "input_data": "test",
                    "writer": {
                        "__spook__": "UnderfolderWriter",
                        "options": {"folder": "/tmp/zizzino/test"},
                    },
                },
            },
            {
                "__spook__": "WriterNode",
                "options": {
                    "input_data": "val",
                    "writer": {
                        "__spook__": "UnderfolderWriter",
                        "options": {"folder": "/tmp/zizzino/val"},
                    },
                },
            },
        ]
    },
}


graph: NodeGraph = Spook.create(graph)

graph.execute()
