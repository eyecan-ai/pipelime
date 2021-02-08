

from pipelime.factories import BeanFactory, GenericFactory
from pipelime.sequences.readers.base import BaseReader
from pipelime.sequences.samples import FileSystemSample, FilesystemItem, Sample
from pipelime.sequences.readers.filesystem import UnderfolderReader
from schema import Schema


def _plug_test(reader: BaseReader):
    """ Test what a generic BaseReader should do

    :param reader: input BaseReader
    :type reader: BaseReader
    """

    assert isinstance(reader, BaseReader)

    print(reader.serialize())
    rereader = reader.hydrate(reader.serialize())
    assert isinstance(rereader, BaseReader)

    factored = BeanFactory.create(reader.serialize())
    assert isinstance(factored, BaseReader)


class TestUnderfolder(object):

    def test_reader(self, toy_dataset_small):

        size = toy_dataset_small['size']
        folder = toy_dataset_small['folder']
        keys = toy_dataset_small['expected_keys']
        root_keys = toy_dataset_small['root_keys']

        copies = [True, False]
        for copy in copies:
            reader = UnderfolderReader(folder=folder, copy_root_files=copy)
            _plug_test(reader)
            assert len(reader) == size

            for sample in reader:
                assert isinstance(sample, Sample)
                assert isinstance(sample, FileSystemSample)

                for key in keys:
                    assert key in sample
                    assert isinstance(sample.metaitem(key), FilesystemItem)

                if copy:
                    for key in root_keys:
                        assert key in sample
                        assert isinstance(sample.metaitem(key), FilesystemItem)
