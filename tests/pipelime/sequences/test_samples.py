import rich
from pipelime.sequences.samples import GroupedSample, PlainSample, SamplesSequence


class TestGroupedSamples(object):

    def test_groupby_samples(self, plain_samples_generator):

        dataset = plain_samples_generator('d0_', 10)

        g = GroupedSample(samples=dataset[::2])

        for sample in dataset:
            for key in sample.keys():
                print(key, sample[key])

        for key in g.keys():
            rich.print(g[key])
