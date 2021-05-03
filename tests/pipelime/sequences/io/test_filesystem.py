

from itertools import product
import uuid
import numpy as np
from pathlib import Path
from pipelime.sequences.operations import OperationFilterKeys
from pipelime.sequences.writers.filesystem import UnderfolderWriter
from pipelime.factories import BeanFactory
from pipelime.sequences.readers.base import BaseReader
from pipelime.sequences.samples import FileSystemSample, FilesystemItem, Sample
from pipelime.sequences.readers.filesystem import UnderfolderReader, UnderfolderReaderTemplate
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


class TestUnderfolderReaderWriterTemplating(object):

    def test_reader_writer(self, toy_dataset_small, tmpdir_factory):

        folder = toy_dataset_small['folder']
        keys = toy_dataset_small['expected_keys']
        root_keys = toy_dataset_small['root_keys']

        reader = UnderfolderReader(folder=folder, copy_root_files=True)
        template = reader.get_filesystem_template()
        assert template is not None
        assert isinstance(template, UnderfolderReaderTemplate)

        assert set(template.extensions_map.keys()) == set(keys + root_keys)
        assert set(template.root_files_keys) == set(root_keys)

        writer_folder = Path(tmpdir_factory.mktemp("writer_folder"))
        writer = UnderfolderWriter(
            folder=writer_folder,
            root_files_keys=template.root_files_keys,
            extensions_map=template.extensions_map,
            zfill=template.idx_length
        )
        writer(reader)
        print("Writer", writer_folder)

        re_reader = UnderfolderReader(folder=writer_folder, copy_root_files=True)
        re_template = re_reader.get_filesystem_template()

        assert set(template.extensions_map.keys()) == set(re_template.extensions_map.keys())
        assert set(template.root_files_keys) == set(re_template.root_files_keys)
        assert template.idx_length == re_template.idx_length

    def test_reader_writer_without_explicit_template(self, toy_dataset_small, tmpdir_factory):

        folder = toy_dataset_small['folder']
        keys = toy_dataset_small['expected_keys']
        root_keys = toy_dataset_small['root_keys']

        reader = UnderfolderReader(folder=folder, copy_root_files=True)
        template = reader.get_filesystem_template()
        assert template is not None
        assert isinstance(template, UnderfolderReaderTemplate)
        assert set(template.extensions_map.keys()) == set(keys + root_keys)
        assert set(template.root_files_keys) == set(root_keys)

        writer_folder = Path(tmpdir_factory.mktemp("writer_folder"))
        writer = UnderfolderWriter(folder=writer_folder)
        writer(reader)
        print("Writer", writer_folder)

        re_reader = UnderfolderReader(folder=writer_folder, copy_root_files=True)
        re_template = re_reader.get_filesystem_template()

        assert set(template.extensions_map.keys()) == set(re_template.extensions_map.keys())
        assert set(template.root_files_keys) == set(re_template.root_files_keys)
        assert template.idx_length == re_template.idx_length

    def test_filtered_reader_writer_without_explicit_template(self, toy_dataset_small, tmpdir_factory):

        folder = toy_dataset_small['folder']
        keys = toy_dataset_small['expected_keys']
        root_keys = toy_dataset_small['root_keys']

        reader = UnderfolderReader(folder=folder, copy_root_files=True)
        op = OperationFilterKeys(keys=keys[0], negate=False)

        filtered_reader = op(reader)

        writer_folder = Path(tmpdir_factory.mktemp("writer_folder"))
        print("Filtered writer", writer_folder)
        writer = UnderfolderWriter(
            folder=writer_folder,
            extensions_map=reader.get_filesystem_template().extensions_map,
            root_files_keys=reader.get_filesystem_template().root_files_keys,
            zfill=reader.get_filesystem_template().idx_length
        )
        writer(filtered_reader)

    def test_writer_copy_correct_extension(self, toy_dataset_small, tmpdir_factory):
        
        folder = toy_dataset_small['folder']
        keys = toy_dataset_small['expected_keys']

        for lazy_samples, copy_files, use_symlinks in product([True, False], repeat=3):
            reader = UnderfolderReader(folder=folder, lazy_samples=lazy_samples)
            extensions_map = reader.get_filesystem_template().extensions_map
            changed_keys = []
            old_ext = 'png'
            new_ext = 'jpg'
            for k, ext in extensions_map.items():
                if ext == old_ext:
                    extensions_map[k] = new_ext
                    changed_keys.append(k)

            writer_folder = Path(tmpdir_factory.mktemp('writer_folder'))
            writer = UnderfolderWriter(
                folder=writer_folder,
                extensions_map=extensions_map,
                root_files_keys=reader.get_filesystem_template().root_files_keys,
                zfill=reader.get_filesystem_template().idx_length,
                copy_files=copy_files,
                use_symlinks=use_symlinks
            )
            writer(reader)

            # if the extension is changed, it should have cached the item
            # even if the writer has copy_files=True
            for sample in reader:
                for k in changed_keys:
                    assert sample.is_cached(k)


class TestUnderfolderReaderWriterConsistency(object):

    def test_reader_writer_content(self, toy_dataset_small, tmpdir_factory):

        # ⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️
        # be careful comparing the content of the samples!
        # For example, make sure they are not JPEGs because they could be compressed
        # when being written and therefore change their numeric content.
        # ⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️

        folder = toy_dataset_small['folder']
        keys = toy_dataset_small['expected_keys']
        root_keys = toy_dataset_small['root_keys']

        combo_items = [
            {'copy_files': True, 'use_symlinks': False},
            {'copy_files': False, 'use_symlinks': False},
            {'copy_files': False, 'use_symlinks': True},
            {'copy_files': True, 'use_symlinks': True},
        ]

        for combo in combo_items:

            reader = UnderfolderReader(folder=folder, copy_root_files=True)
            template = reader.get_filesystem_template()

            print("\nCombo", combo)
            writer_folder = Path(tmpdir_factory.mktemp(str(uuid.uuid1())))
            print(writer_folder)
            writer = UnderfolderWriter(
                folder=writer_folder,
                copy_files=combo['copy_files'],
                use_symlinks=combo['use_symlinks']
            )
            writer(reader)

            re_reader = UnderfolderReader(folder=writer_folder, copy_root_files=True)
            re_template = re_reader.get_filesystem_template()

            for idx in range(len(re_reader)):
                data = reader[idx]['image']
                re_data = re_reader[idx]['image']
                assert np.array_equal(data, re_data)
