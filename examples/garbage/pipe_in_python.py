import rich
from pipelime.lib import AddOp, PlainSample, SamplesSequence, SequenceOpFactory
from choixe.configurations import XConfig


def decorate_with_ops():
    def add(self, o: SamplesSequence):
        return AddOp()([self, o])

    SamplesSequence.__add__ = add
    SamplesSequence.aaa = add


decorate_with_ops()
print(SamplesSequence.__add__)


N = 10
samples_a = [
    PlainSample(
        data={"idx": idx, "metadata": {"idx": idx, "name": str(idx), "label": 0}}
    )
    for idx in range(N)
]
samples_b = [
    PlainSample(
        data={"idx": idx, "metadata": {"idx": idx, "name": str(idx), "label": 1}}
    )
    for idx in range(N)
]

dataset_a = SamplesSequence(samples=samples_a)
dataset_b = SamplesSequence(samples=samples_b)


d = dataset_a + dataset_b
print(len(d))
