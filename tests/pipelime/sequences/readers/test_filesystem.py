

from pipelime.sequences.samples import FileSystemSample, FilesystemItem, Sample
from pipelime.sequences.readers.filesystem import UnderfolderReader


class TestUnderfolder(object):

    def test_reader(self, toy_dataset_small):
        print("OK", toy_dataset_small)

        size = toy_dataset_small['size']
        folder = toy_dataset_small['folder']
        keys = toy_dataset_small['expected_keys']

        reader = UnderfolderReader(folder=folder)
        assert len(reader) == size

        for sample in reader:
            assert isinstance(sample, Sample)
            assert isinstance(sample, FileSystemSample)

            for key in keys:
                assert key in sample
                assert isinstance(sample.metaitem(key), FilesystemItem)
