from numpy.lib.utils import source
from pipelime.lib import (
    AddOp,
    Dict2ListOp,
    FileSystemSample,
    FilterByQueryOp,
    PlainSample,
    Sample,
    SamplesSequence,
    ShuffleOp,
    SplitByQueryOp,
    SplitsOp,
    SubsampleOp,
)
import sys
from memory_profiler import profile
from pathlib import Path

# @profile


def my_func():

    dataset_folder = Path(
        "/home/daniele/Desktop/experiments/2021-01-28.PlaygroundDatasets/lego_00"
    )
    fmap = {
        "image": dataset_folder / "data/00000_image.jpg",
        "xray": dataset_folder / "data/00000_xray.png",
        "metadata": dataset_folder / "data/00000_metadata.yml",
        "pose": dataset_folder / "data/00000_pose.txt",
    }

    # s = FileSystemSample(data_map=fmap, preload=True)
    # s = PlainSample(data={'a': 2})
    samples = [
        PlainSample(data={"idx": idx, "metadata": {"idx": idx, "name": str(idx)}})
        for idx in range(25)
    ]
    # samples = [FileSystemSample(data_map=fmap) for idx in range(250)]
    d = SamplesSequence(samples=samples)

    op = AddOp.build_from_dict({"type": "AddOp", "options": {}})

    op_sub = SubsampleOp.build_from_dict(
        {"type": "SubsampleOp", "options": {"factor": 0.5}}
    )

    op_shuf = ShuffleOp.build_from_dict({"type": "ShuffleOp", "options": {"seed": -1}})

    op_splits = SplitsOp.build_from_dict(
        {
            "type": "SplitsOp",
            "options": {"split_map": {"train": 0.5, "val": 0.25, "test": 0.25}},
        }
    )

    op_d2l = Dict2ListOp.build_from_dict({"type": "Dict2ListOp", "options": {}})

    op_query = FilterByQueryOp.build_from_dict(
        {"type": "FilterByQueryOp", "options": {"query": "`metadata.idx` < 5"}}
    )

    op_query2 = SplitByQueryOp.build_from_dict(
        {"type": "SplitByQueryOp", "options": {"query": "`metadata.idx` < 5"}}
    )

    d = op([d, d, d])
    d = op_shuf(d)
    print(len(d))

    a, b = op_query2(d)
    print(len(a), len(b))

    d = op_query(d)
    print(len(d))

    d = op_sub(d)
    print(len(d))

    out = op_splits(d)
    for k, v in out.items():
        print(k, len(v))

    out_l = op_d2l(out)
    for d in out_l:
        print(len(d))

    # op2 = AddOp()
    # op3 = ShuffleOp()
    # op4 = SplitsOp(split_map={'train': 0.6, 'val': 0.18, 'test': 0.22})
    # op5 = Dict2ListOp()

    # op_q = FilterByQueryOp(query="`metadata.name` CONTAINS '2'")

    # print(len(d))
    # d = op_q(d)
    # print(len(d))

    # for s in d:
    #     print(s)

    # op.print()
    # op4.print()
    # op5.print()

    # print(op.match(op4))
    # d_sum = op([d, d, d, d])

    # print(len(d_sum))
    # d_split = op4(d_sum)
    # for k, v in d_split.items():
    #     print(k, len(v))

    # for s in op5(d_split):
    #     print(len(s))
    # a = op.input_port()
    # b = op.input_port()
    # print(a.json_schema(0) == b.json_schema(0))
    # print(len(d))
    # print(len(d_sum))


if __name__ == "__main__":
    my_func()
