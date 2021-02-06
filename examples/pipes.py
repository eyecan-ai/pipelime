

from pipelime.factories import GenericFactory
from pipelime.sequences.pipes import NodeGraph
pipe = {
    'type': 'NodeGraph',
    'nodes': [
        {
            'type': 'SourceNode',
            'options': {
                'output_data': 'A',
                'sequence': {
                    'type': 'UnderfolderReader',
                    'options': {
                        'folder': '/Users/daniele/Downloads/lego_dataset/lego_00',
                    }
                }
            }
        },
        {
            'type': 'SourceNode',
            'options': {
                'output_data': 'B',
                'sequence': {
                    'type': 'UnderfolderReader',
                    'options': {
                        'folder': '/Users/daniele/Downloads/lego_dataset/lego_01'
                    }
                }
            }
        },
        {
            'type': 'OperationNode',
            'options': {
                'input_data': 'A',
                'output_data': 'A_filtered',
                'operation': {
                    'type': 'OperationFilterByQuery',
                    'options': {
                        'query': '`metadata.tag` == "image"'
                    }
                }
            }
        },
        {
            'type': 'OperationNode',
            'options': {
                'input_data': ['A_filtered', 'B'],
                'output_data': 'C',
                'operation': {
                    'type': 'OperationSum',
                    'options': {}
                }
            }
        },
        {
            'type': 'OperationNode',
            'options': {
                'input_data': 'C',
                'output_data': 'D',
                'operation': {
                    'type': 'OperationSplits',
                    'options': {
                        'split_map': {
                            'train': 0.45,
                            'val': 0.45,
                            'test': 0.1
                        }
                    }
                }
            }
        },
    ]
}

print(GenericFactory.FACTORY_MAP)

graph: NodeGraph = NodeGraph.build_from_dict(pipe)

f, a = graph.draw_to_file()
print(f, a)
