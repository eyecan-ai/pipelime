from numpy.lib.utils import source
from pipelime.lib import FileSystemSample, PlainSample, Sample, SamplesSequence
import sys
from memory_profiler import profile
from pathlib import Path

# @profile


def my_func():

    dataset_folder = Path('/home/daniele/Desktop/experiments/2021-01-28.PlaygroundDatasets/lego_00')
    fmap = {
        'image': dataset_folder / 'data/00000_image.jpg',
        'xray': dataset_folder / 'data/00000_xray.png',
        'metadata': dataset_folder / 'data/00000_metadata.yml',
        'pose': dataset_folder / 'data/00000_pose.txt'
    }

    # s = FileSystemSample(data_map=fmap, preload=True)
    # s = PlainSample(data={'a': 2})
    #samples = [PlainSample(data={'idx': idx, 'v': str(idx)}) for idx in range(25)]
    samples = [FileSystemSample(data_map=fmap) for idx in range(250)]
    d = SamplesSequence(samples=samples)
    d = d + d
    d += d
    d = d % 4
    d = d % "`metadata.counter` == 0"
    d = d % 0.5
    d = d.shuffle()

    d_a, d_b = d.splits(percentages=(0.51, 0.49))
    d_a, d_b = d / (0.51, 0.49)

    d_out = d / {'train': 0.9, 'val': 0.1}
    for k, v in d_out.items():
        print(k, len(v))

    # for idx in range(len(d)):
    #     d[idx]['gino'] = 2.22

    # for s in d:
    #     s: Sample
    #     print(list(s.keys()))

    print(len(d), len(d_a), len(d_b))


if __name__ == '__main__':
    my_func()
