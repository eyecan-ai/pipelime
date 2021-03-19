import numpy as np
from pipelime.sequences.readers.filesystem import UnderfolderReader
import rich
from pipelime.sequences.samples import FileSystemSample, FilesystemItem, GroupedSample, MemoryItem, PlainSample, SamplesSequence


class TestPlainSamples(object):

    def test_plain_sample(self, plain_samples_generator):

        dataset = plain_samples_generator('d0_', 10)
        for sample in dataset:
            assert set(sample.keys()) == set(sample.skeleton.keys())
            for key in sample.keys():
                assert isinstance(sample.metaitem(key), MemoryItem)


class TestGroupedSamples(object):

    def _empty(*args):
        pass

    def test_groupby_samples(self, plain_samples_generator):

        dataset = plain_samples_generator('d0_', 10)

        samples = dataset[::2]
        g = GroupedSample(samples=samples)
        g_copy = g.copy()

        for key in g.keys():
            self._empty(key, g[key], len(g))
            assert isinstance(g.metaitem(key), type(samples[0].metaitem(key)))

        g.rename('image', 'NEW_IMAGE')
        assert 'image' not in g

        for key in list(g.keys()):
            del g[key]

        assert len(g.keys()) == 0
        assert len(g_copy.keys()) == 0

        g['my_new_key'] = 112.3
        assert len(g.keys()) == 1

        assert len(GroupedSample(samples=[])) == 0
        assert len(GroupedSample(samples=[]).keys()) == 0


class TestFilesystemSample(object):

    def test_filesystem_sample(self, filesystem_datasets, tmp_path_factory):

        dataset_folder = filesystem_datasets['minimnist_underfolder']['folder']

        reader = UnderfolderReader(folder=dataset_folder)

        sample = reader[0]

        for key in sample.keys():
            assert isinstance(sample.metaitem(key), FilesystemItem)

        assert len(sample) > 0
        assert isinstance(sample['image'], np.ndarray)
        assert isinstance(sample, FileSystemSample)

        sample.rename('image', '_NEW_IMAGE_')
        assert 'image' not in sample

        del sample['_NEW_IMAGE_']

    def test_filesystem_sample_nonlazy(self, filesystem_datasets, tmp_path_factory):

        dataset_folder = filesystem_datasets['minimnist_underfolder']['folder']
        reader = UnderfolderReader(folder=dataset_folder, lazy_samples=False)
