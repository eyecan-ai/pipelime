

from pipelime.factories import BeanFactory
from pipelime.sequences.pipes import NodeGraph

graph = {
    '__type__': 'NodeGraph',
    'options': {
        'nodes': [
            {
                '__type__': 'ReaderNode',
                'options': {
                    'output_data': 'A',
                    'reader': {
                        '__type__': 'UnderfolderReader',
                        'options': {
                            'folder': '/Users/daniele/Downloads/lego_dataset/lego_00',
                        }
                    }
                }
            },
            {
                '__type__': 'ReaderNode',
                'options': {
                    'output_data': 'B',
                    'reader': {
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
                    'output_data': 'A1',
                    'operation': {
                        '__type__': 'OperationFilterByQuery',
                        'options': {
                            'query': '`metadata.tag` == "black"'
                        }
                    }
                }
            },

            {
                '__type__': 'OperationNode',
                'options': {
                    'input_data': ['A1', 'B'],
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
                        '__type__': 'OperationFilterByQuery',
                        'options': {
                            'query': '`metadata.tag` == "black"'
                        }
                    }
                }
            },
            {
                '__type__': 'OperationNode',
                'options': {
                    'input_data': 'D',
                    'output_data': 'D1',
                    'operation': {
                        '__type__': 'OperationSubsample',
                        'options': {
                            'factor': 0.1
                        }
                    }
                }
            },
            {
                '__type__': 'OperationNode',
                'options': {
                    'input_data': 'D1',
                    'output_data': {
                        'a': 'train',
                        'b': 'test',
                        'c': 'val'
                    },
                    'operation': {
                        '__type__': 'OperationSplits',
                        'options': {
                            'split_map': {
                                'a': 0.45,
                                'b': 0.45,
                                'c': 0.1
                            }
                        }
                    }
                }
            },
            {
                '__type__': 'WriterNode',
                'options': {
                    'input_data': 'train',
                    'writer': {
                        '__type__': 'UnderfolderWriter',
                        'options': {
                            'folder': '/tmp/zizzino/train'
                        }
                    }
                }
            },
            {
                '__type__': 'WriterNode',
                'options': {
                    'input_data': 'test',
                    'writer': {
                        '__type__': 'UnderfolderWriter',
                        'options': {
                            'folder': '/tmp/zizzino/test'
                        }
                    }
                }
            },
            {
                '__type__': 'WriterNode',
                'options': {
                    'input_data': 'val',
                    'writer': {
                        '__type__': 'UnderfolderWriter',
                        'options': {
                            'folder': '/tmp/zizzino/val'
                        }
                    }
                }
            },
        ]
    }
}


graph: NodeGraph = BeanFactory.create(graph)

graph.execute()
# f, a = graph.draw_to_file()
# print(f, a)
