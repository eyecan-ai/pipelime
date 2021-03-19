

import hashlib
import random
import os
import tempfile
from networkx.drawing.nx_agraph import graphviz_layout, to_agraph
from typing import Union
import rich
from pipelime.lib import AddOp, PlainSample, SamplesSequence, SequenceOpFactory
from choixe.configurations import XConfig
import networkx as nx
import matplotlib.pyplot as plt


def sample_dataset(namespace: str, N: int):

    samples = []
    for i in range(N):
        data = {
            'idx': f'{namespace}{i}',
            'number': i,
            'fraction': i / 1000.,
            'metadata': {
                'name': f'{namespace}{i}',
                'N': i,
                'deep': {
                    'super_deep': 0
                }
            }
        }
        samples.append(PlainSample(data=data))

    return SamplesSequence(samples=samples)


class PipeNode(object):
    ID_COUNTER = 0

    def __init__(self, input_ports: Union[str, list, dict], output_ports: Union[str, list, dict], node: dict) -> None:
        self.id = PipeNode.ID_COUNTER
        PipeNode.ID_COUNTER += 1
        self.input_ports = input_ports
        self.output_ports = output_ports
        self.node = node

    def in_ports(self):
        return self.ports_as_list(self.input_ports)

    def out_ports(self):
        return self.ports_as_list(self.output_ports)

    def ports_as_list(self, v: any):
        if v is None:
            return []
        elif isinstance(v, str):
            return [v]
        elif isinstance(v, list):
            return v
        elif isinstance(v, dict):
            return list(v.values())
        else:
            raise NotImplementedError(f'{type(v)}')

    def __repr__(self) -> str:
        return str(self.id)


class DataNode(object):

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return self.name

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return self.name == o.name


pipe = {
    'input': {
        'GOOD': None,
        'BAD': None
    },
    'modules': [
        {
            'input': None,
            'output': 'GOOD',
            'node': {
                'type': 'Source',
                'data': sample_dataset('good_', 100)
            }
        },
        {
            'input': None,
            'output': 'BAD',
            'node': {
                'type': 'Source',
                'data': sample_dataset('bad_', 100)
            }
        },
        # {
        #     'input': None,
        #     'output': 'SUPER',
        #     'node': {
        #         'type': 'Source',
        #         'data': sample_dataset('super_', 100)
        #     }
        # },
        {
            'input': 'BAD',
            'output': {
                'a': 'BAD_0',
                'b': 'BAD_1'
            },
            'node': {
                'type': 'SplitsOp',
                'options': {
                    'split_map': {
                        'a': 0.5,
                        'b': 0.5
                    }
                }
            }
        },
        {
            'input': 'GOOD',
            'output': {
                'a': 'GOOD_0',
                'b': 'GOOD_1'
            },
            'node': {
                'type': 'SplitsOp',
                'options': {
                    'split_map': {
                        'a': 0.5,
                        'b': 0.5
                    }
                }
            }
        },
        {
            'input': ['BAD_0', 'GOOD_0'],
            'output': 'train',
            'node': {
                'type': 'AddOp',
                'options': {}
            }
        },
        {
            'input': ['BAD_1', 'GOOD_1'],  # 'SUPER'],
            'output': 'testval',
            'node': {
                'type': 'AddOp',
                'options': {}
            }
        },
        {
            'input': 'testval',
            'output': '_t',
            'node': {
                'type': 'ShuffleOp',
                'options': {
                    'seed': -1
                }
            }
        },
        {
            'input': '_t',
            'output': '_t2',
            'node': {
                'type': 'SubsampleOp',
                'options': {
                    'factor': 2
                }
            }
        },
        {
            'input': '_t2',
            'output': {
                'a': 'test',
                'b': 'val'
            },
            'node': {
                'type': 'SplitsOp',
                'options': {
                    'split_map': {
                        'a': 0.5,
                        'b': 0.5
                    }
                }
            }
        },
        # {
        #     'input': 's',
        #     'output': 's',
        #     'node': {
        #         'type': 'SubsampleOp',
        #         'options': {
        #             'factor': 0.5
        #         }
        #     }
        # },
        # {
        #     'input': 's',
        #     'output': {
        #         'a': 'train',
        #         'b': 'val',
        #         'c': 'test',
        #     },
        #     'node': {
        #         'type': 'SplitsOp',
        #         'options': {
        #             'split_map': {
        #                 'a': 0.8,
        #                 'b': 0.1,
        #                 'c': 0.1
        #             }
        #         }
        #     }
        # }
    ]
}


g = nx.DiGraph()

modules = pipe['modules']
random.shuffle(modules)
nodes = []
for m in pipe['modules']:

    node = PipeNode(
        input_ports=m['input'],
        output_ports=m['output'],
        node=m['node']
    )
    nodes.append(node)

    print("NODE", node.id, node.in_ports(), node.out_ports())

    [g.add_edge(DataNode(x), node) for x in node.in_ports()]
    [g.add_edge(node, DataNode(x)) for x in node.out_ports()]


# nx.draw_spring(g, with_labels=True)
# plt.show()

def prepare_in_data(node: PipeNode, cache: dict):
    v = node.input_ports
    if v is None:
        return None
    elif isinstance(v, str):
        return cache[v]
    elif isinstance(v, list):
        return [cache[x] for x in v]
    elif isinstance(v, dict):
        return {k: cache[v] for k, v in v.items()}
    else:
        raise NotImplementedError(f'{type(v)}')


def prepare_out_data(o: Union[SamplesSequence, list, dict], node: PipeNode, cache: dict):
    v = node.output_ports
    if v is None:
        pass
    elif isinstance(v, str):
        cache[v] = o
    elif isinstance(v, list):
        for idx, name in enumerate(v):
            cache[name] = o[idx]
    elif isinstance(v, dict):
        for k, name in v.items():
            cache[name] = o[k]
    else:
        raise NotImplementedError(f'{type(v)}')


_cache = {}
A = nx.topological_sort(g)
# for a in A:
#     print("N: ", a)
for a in A:
    print("N: ", a)
    if isinstance(a, PipeNode):
        if a.node['type'] != 'Source':
            op = SequenceOpFactory.create(a.node)
            data = prepare_in_data(a, _cache)
            out = op(data)
            prepare_out_data(out, a, _cache)
        else:
            _cache[a.output_ports] = a.node['data']


for k, v in _cache.items():
    print(k, len(v))

A = to_agraph(g)
print(A)
A.layout('dot')
fname = f'{tempfile.NamedTemporaryFile().name}.png'
print(fname)
A.draw(fname)
os.system(f'open {fname}')
