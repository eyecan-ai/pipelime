

from pipelime.factories import BeanFactory, GenericFactory
from pipelime.sequences.pipes import NodeGraph

pipe = {
    '__type__': 'NodeGraph',
    'options': {
        'nodes': [
            {
                '__type__': 'SourceNode',
                'options': {
                    'output_data': 'A',
                    'sequence': {
                        '__type__': 'UnderfolderReader',
                        'options': {
                            'folder': '/Users/daniele/Downloads/lego_dataset/lego_00',
                        }
                    }
                }
            },
            {
                '__type__': 'SourceNode',
                'options': {
                    'output_data': 'B',
                    'sequence': {
                        '__type__': 'UnderfolderReader',
                        'options': {
                            'folder': '/Users/daniele/Downloads/lego_dataset/lego_01'
                        }
                    }
                }
            },
            {
                '__type__': 'OperationNode',
                'options': {
                    'input_data': 'A',
                    'output_data': 'A_filtered',
                    'operation': {
                        '__type__': 'OperationFilterByQuery',
                        'options': {
                            'query': '`metadata.tag` == "image"'
                        }
                    }
                }
            },
            {
                '__type__': 'OperationNode',
                'options': {
                    'input_data': ['A_filtered', 'B'],
                    'output_data': 'C',
                    'operation': {
                        '__type__': 'OperationSum',
                        'options': {}
                    }
                }
            },
            {
                '__type__': 'OperationNode',
                'options': {
                    'input_data': 'C',
                    'output_data': 'D',
                    'operation': {
                        '__type__': 'OperationSplits',
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
}


graph: NodeGraph = BeanFactory.create(pipe)

f, a = graph.draw_to_file()
print(f, a)
