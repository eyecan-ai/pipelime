

import rich
from pipelime.lib import AddOp, PlainSample, SamplesSequence, SequenceOpFactory
from choixe.configurations import XConfig

pipe = {
    'input': {
        'X': None
    },
    'modules': [
        {
            'input': 'X',
            'output': ['GOOD', 'BAD'],
            'op': {
                'type': 'SplitByQueryOp',
                'options': {
                    'query': '`metadata.label` == 0'
                }
            }
        },
        # {
        #     'input': 'BAD',
        #     'output': {
        #         'a': 'BAD_0',
        #         'b': 'BAD_1'
        #     },
        #     'op': {
        #         'type': 'SplitsOp',
        #         'options': {
        #             'split_map': {
        #                 'a': 0.5,
        #                 'b': 0.5
        #             }
        #         }
        #     }
        # },
        # {
        #     'input': 'GOOD',
        #     'output': {
        #         'a': 'GOOD_0',
        #         'b': 'GOOD_1'
        #     },
        #     'op': {
        #         'type': 'SplitsOp',
        #         'options': {
        #             'split_map': {
        #                 'a': 0.5,
        #                 'b': 0.5
        #             }
        #         }
        #     }
        # },
        # {
        #     'input': ['BAD_0', 'GOOD_0'],
        #     'output': 'train',
        #     'op': {
        #         'type': 'AddOp',
        #         'options': {}
        #     }
        # },
        # {
        #     'input': ['BAD_1', 'GOOD_1'],
        #     'output': 'testval',
        #     'op': {
        #         'type': 'AddOp',
        #         'options': {}
        #     }
        # },
        # {
        #     'input': 'testval',
        #     'output': 'testval',
        #     'op': {
        #         'type': 'ShuffleOp',
        #         'options': {
        #             'seed': -1
        #         }
        #     }
        # },
        # {
        #     'input': 'testval',
        #     'output': {
        #         'a': 'test',
        #         'b': 'val'
        #     },
        #     'op': {
        #         'type': 'SplitsOp',
        #         'options': {
        #             'split_map': {
        #                 'a': 0.5,
        #                 'b': 0.5
        #             }
        #         }
        #     }
        # },
        # {
        #     'input': 's',
        #     'output': 's',
        #     'op': {
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
        #     'op': {
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

cfg = XConfig.from_dict(pipe)
cfg.save_to('/tmp/cfgs/gino.yml')

pipe = XConfig(filename='/tmp/cfgs/gino.yml')
rich.print(SequenceOpFactory.FACTORY_MAP)

N = 10
samples_a = [PlainSample(data={'idx': idx, 'metadata': {'idx': idx, 'name': str(idx), 'label': 0}}) for idx in range(N)]
samples_b = [PlainSample(data={'idx': idx, 'metadata': {'idx': idx, 'name': str(idx), 'label': 1}}) for idx in range(N)]
samples = samples_a + samples_b

dataset = SamplesSequence(samples=samples)


pipe['input']['X'] = dataset

data_map = {}


def check_input(i):
    if isinstance(i, str):
        assert i in data_map, f'Input "{i}" not present'
    elif isinstance(i, list):
        [check_input(x) for x in i]
    elif isinstance(i, dict):
        [check_input(x) for x, _ in i.items()]


def parse_input(i):
    if isinstance(i, str):
        return data_map[i]
    elif isinstance(i, list):
        return [parse_input(x) for x in i]
    elif isinstance(i, dict):
        return {k: parse_input(x) for k, x in i.items()}


def push_output(o, data):
    if isinstance(o, str):
        data_map[o] = data
    elif isinstance(o, list):
        for idx, name in enumerate(o):
            data_map[name] = data[idx]
    elif isinstance(o, dict):
        for k, name in o.items():
            data_map[name] = data[k]


for name, d in pipe['input'].items():
    data_map[name] = d

for m in pipe['modules']:
    i = m['input']
    o = m['output']
    op = SequenceOpFactory.create(m['op'])

    try:
        check_input(i)
        i = parse_input(i)

        out = op(i)
        push_output(o, out)
    except Exception as e:
        print(f"Error on module: {m}. {e}")
        break

for k, v in data_map.items():
    print(k, len(v))
    for s in v:
        print(s)
