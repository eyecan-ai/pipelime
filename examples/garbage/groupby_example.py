import rich
from pipelime.sequences.samples import PlainSample, SamplesSequence
import numpy as np

from pipelime.sequences.operations import OperationGroupBy


def generate_plain_samples(
    namespace: str, N: int, group_each: int = 5, heavy_data: bool = False
):

    samples = []
    for i in range(N):
        data = {
            "idx": f"{namespace}{i}",
            "number": i,
            "fraction": i / 1000.0,
            "odd": i % 2 == 1,
            "data0": np.random.uniform(0.0, 1.0, (64, 64, 3))
            if heavy_data
            else 1.64643,
            "data1": np.random.uniform(0, 255, (8, 8)).astype(np.uint8)
            if heavy_data
            else 25588,
            "metadata": {
                "even": i % 2 == 0,
                "name": f"{namespace}{i}",
                "N": i,
                "deep": {
                    "super_deep": 0,
                    "groupby_field": int(i / group_each),
                    "groupby_field2": int(i % 3),
                    "groupby_field3": int(i % 10),
                },
            },
        }
        samples.append(PlainSample(data=data))

    return samples


d = SamplesSequence(samples=generate_plain_samples("d0", 128))

op = OperationGroupBy(field="metadata.deep.groupby_field")

out = op(d)
for sample in out:
    for k, v in sample.items():
        rich.print(k, v)

op2 = OperationGroupBy(field="metadata.deep.groupby_field")

out2 = op2(out)
# for sample in out2:
#     for k, v in sample.items():
#         rich.print(k, v)

op3 = OperationGroupBy(field="metadata.even.0.0")

out3 = op3(out2)
# for sample in out3:
#     for k, v in sample.items():
#         rich.print(k, v)
print(len(out), len(out2), len(out3))
