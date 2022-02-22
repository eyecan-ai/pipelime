import uuid
from itertools import product
from pathlib import Path
import os
import numpy as np
from choixe.spooks import Spook
import rich
import networkx as nx
from pipelime.sequences.operations import OperationFilterKeys
from pipelime.sequences.readers.base import BaseReader, ReaderTemplate
from pipelime.sequences.readers.filesystem import (
    UnderfolderLinksPlugin,
    UnderfolderReader,
)
from pipelime.sequences.samples import FileSystemItem, FileSystemSample, Sample
from pipelime.sequences.writers.filesystem import UnderfolderWriter, UnderfolderWriterV2
import pytest


def _plug_test(reader: BaseReader):
    """Test what a generic BaseReader should do

    :param reader: input BaseReader
    :type reader: BaseReader
    """

    assert isinstance(reader, BaseReader)

    print(reader.serialize())
    rereader = reader.hydrate(reader.serialize())
    assert isinstance(rereader, BaseReader)

    factored = Spook.create(reader.serialize())
    assert isinstance(factored, BaseReader)


class TestUnderfolder(object):
    def test_reader(self, toy_dataset_small):

        size = toy_dataset_small["size"]
        folder = toy_dataset_small["folder"]
        keys = toy_dataset_small["expected_keys"]
        root_keys = toy_dataset_small["root_keys"]

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
                    assert isinstance(sample.metaitem(key), FileSystemItem)

                if copy:
                    for key in root_keys:
                        assert key in sample
                        assert isinstance(sample.metaitem(key), FileSystemItem)


class TestUnderfolderReaderWriterTemplating(object):
    def test_reader_writer(self, toy_dataset_small, tmpdir_factory):

        folder = toy_dataset_small["folder"]
        keys = toy_dataset_small["expected_keys"]
        root_keys = toy_dataset_small["root_keys"]

        reader = UnderfolderReader(folder=folder, copy_root_files=True)
        template = reader.get_reader_template()
        _helper_template = UnderfolderReader.get_reader_template_from_folder(
            folder=folder
        )
        assert _helper_template == template

        assert template is not None
        assert isinstance(template, ReaderTemplate)

        assert set(template.extensions_map.keys()) == set(keys + root_keys)
        assert set(template.root_files_keys) == set(root_keys)

        writer_folder = Path(tmpdir_factory.mktemp("writer_folder"))
        writer = UnderfolderWriter(
            folder=writer_folder,
            root_files_keys=template.root_files_keys,
            extensions_map=template.extensions_map,
            zfill=template.idx_length,
        )
        writer(reader)
        print("Writer", writer_folder)

        re_reader = UnderfolderReader(folder=writer_folder, copy_root_files=True)
        re_template = re_reader.get_reader_template()

        assert set(template.extensions_map.keys()) == set(
            re_template.extensions_map.keys()
        )
        assert set(template.root_files_keys) == set(re_template.root_files_keys)
        assert template.idx_length == re_template.idx_length

    def test_reader_writer_without_explicit_template(
        self, toy_dataset_small, tmpdir_factory
    ):

        folder = toy_dataset_small["folder"]
        keys = toy_dataset_small["expected_keys"]
        root_keys = toy_dataset_small["root_keys"]

        reader = UnderfolderReader(folder=folder, copy_root_files=True)
        template = reader.get_reader_template()
        assert template is not None
        assert isinstance(template, ReaderTemplate)
        assert set(template.extensions_map.keys()) == set(keys + root_keys)
        assert set(template.root_files_keys) == set(root_keys)

        writer_folder = Path(tmpdir_factory.mktemp("writer_folder"))
        writer = UnderfolderWriter(folder=writer_folder)
        writer(reader)
        print("Writer", writer_folder)

        re_reader = UnderfolderReader(folder=writer_folder, copy_root_files=True)
        re_template = re_reader.get_reader_template()

        assert set(template.extensions_map.keys()) == set(
            re_template.extensions_map.keys()
        )
        assert set(template.root_files_keys) == set(re_template.root_files_keys)
        assert template.idx_length == re_template.idx_length

    def test_filtered_reader_writer_without_explicit_template(
        self, toy_dataset_small, tmpdir_factory
    ):

        folder = toy_dataset_small["folder"]
        keys = toy_dataset_small["expected_keys"]
        # root_keys = toy_dataset_small["root_keys"]

        reader = UnderfolderReader(folder=folder, copy_root_files=True)
        op = OperationFilterKeys(keys=keys[0], negate=False)

        filtered_reader = op(reader)

        writer_folder = Path(tmpdir_factory.mktemp("writer_folder"))
        print("Filtered writer", writer_folder)
        writer = UnderfolderWriter(
            folder=writer_folder,
            extensions_map=reader.get_reader_template().extensions_map,
            root_files_keys=reader.get_reader_template().root_files_keys,
            zfill=reader.get_reader_template().idx_length,
        )
        writer(filtered_reader)

    def test_writer_copy_correct_extension(self, toy_dataset_small, tmpdir_factory):

        folder = toy_dataset_small["folder"]
        # keys = toy_dataset_small["expected_keys"]

        for lazy_samples, copy_files, use_symlinks in product([True, False], repeat=3):
            reader = UnderfolderReader(folder=folder, lazy_samples=lazy_samples)
            extensions_map = reader.get_reader_template().extensions_map
            changed_keys = []
            old_ext = "png"
            new_ext = "jpg"
            for k, ext in extensions_map.items():
                if ext == old_ext:
                    extensions_map[k] = new_ext
                    changed_keys.append(k)

            writer_folder = Path(tmpdir_factory.mktemp("writer_folder"))
            writer = UnderfolderWriter(
                folder=writer_folder,
                extensions_map=extensions_map,
                root_files_keys=reader.get_reader_template().root_files_keys,
                zfill=reader.get_reader_template().idx_length,
                copy_files=copy_files,
                use_symlinks=use_symlinks,
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

        folder = toy_dataset_small["folder"]
        # keys = toy_dataset_small["expected_keys"]
        # root_keys = toy_dataset_small["root_keys"]

        combo_items = [
            {"copy_files": True, "use_symlinks": False},
            {"copy_files": False, "use_symlinks": False},
            {"copy_files": False, "use_symlinks": True},
            {"copy_files": True, "use_symlinks": True},
        ]

        for combo in combo_items:

            reader = UnderfolderReader(folder=folder, copy_root_files=True)
            reader.get_reader_template()

            print("\nCombo", combo)
            writer_folder = Path(tmpdir_factory.mktemp(str(uuid.uuid1())))
            print(writer_folder)
            writer = UnderfolderWriter(
                folder=writer_folder,
                copy_files=combo["copy_files"],
                use_symlinks=combo["use_symlinks"],
            )
            writer(reader)

            re_reader = UnderfolderReader(folder=writer_folder, copy_root_files=True)
            re_reader.get_reader_template()

            for idx in range(len(re_reader)):
                data = reader[idx]["image"]
                re_data = re_reader[idx]["image"]
                assert np.array_equal(data, re_data)


class TestUnderfolderWriterForceCopy(object):
    def test_reader_writer_force_copy(self, toy_dataset_small, tmpdir_factory):
        folder = toy_dataset_small["folder"]
        image_key = "image"

        reader = UnderfolderReader(folder=folder, copy_root_files=True)
        sample: FileSystemSample = reader[0]
        original_image = sample[image_key].copy()
        sample[image_key] = 255 - original_image
        assert sample.is_cached(image_key)
        assert np.any(sample[image_key] != original_image)

        writer_folder = Path(tmpdir_factory.mktemp(str(uuid.uuid1())))
        print(writer_folder)
        writer = UnderfolderWriter(
            folder=writer_folder,
            copy_files=True,
            use_symlinks=False,
            force_copy_keys=[image_key],
        )
        writer(reader)

        re_reader = UnderfolderReader(folder=writer_folder, copy_root_files=True)
        re_sample = re_reader[0]
        assert np.all(re_sample[image_key] == original_image)


class TestUnderfolderWriterMultiprocessing(object):
    def test_reader_writer_force_copy(self, toy_dataset_small, tmpdir_factory):
        folder = toy_dataset_small["folder"]

        reader = UnderfolderReader(folder=folder, copy_root_files=True)

        workers_options = [-1, 0, 1, 2, 3, 4]

        for num_workers in workers_options:
            writer_folder = Path(tmpdir_factory.mktemp(str(uuid.uuid1())))
            print(writer_folder)
            writer = UnderfolderWriter(
                folder=writer_folder,
                copy_files=True,
                use_symlinks=False,
                num_workers=num_workers,
            )
            writer(reader)

            re_reader = UnderfolderReader(folder=writer_folder, copy_root_files=True)
            re_sample = re_reader[0]
            assert isinstance(re_sample, Sample)
            assert len(reader) == len(re_reader)
            for sample_id in range(len(re_reader)):
                sample = reader[sample_id]
                resample = re_reader[sample_id]
                assert sample["metadata"]["id"] == resample["metadata"]["id"]


class TestUnderfolderReaderMultiprocessing(object):
    def test_reader_writer_force_copy(self, toy_dataset_small, tmpdir_factory):
        folder = toy_dataset_small["folder"]
        # image_key = "image"

        workers_options = [1, 0, 1, 2, 3, 4]

        for num_workers in workers_options:
            reader = UnderfolderReader(
                folder=folder, copy_root_files=True, num_workers=num_workers
            )
            writer_folder = Path(tmpdir_factory.mktemp(str(uuid.uuid1())))
            print(writer_folder)
            writer = UnderfolderWriter(
                folder=writer_folder,
                copy_files=True,
                use_symlinks=False,
                num_workers=num_workers,
            )
            writer(reader)

            re_reader = UnderfolderReader(folder=writer_folder, copy_root_files=True)
            re_sample = re_reader[0]
            assert isinstance(re_sample, Sample)
            assert len(reader) == len(re_reader)
            for sample_id in range(len(re_reader)):
                sample = reader[sample_id]
                resample = re_reader[sample_id]
                assert sample["metadata"]["id"] == resample["metadata"]["id"]


class TestUnderfolderWriterSymlinks(object):
    def test_symlinks_not_broken(self, toy_dataset_small, tmpdir_factory):
        folder = toy_dataset_small["folder"]

        os.chdir(folder.parent)
        folder = folder.name

        reader = UnderfolderReader(folder=folder)
        writer_folder = Path(tmpdir_factory.mktemp(str(uuid.uuid1())))
        writer = UnderfolderWriter(
            folder=writer_folder,
            copy_files=True,
            use_symlinks=True,
        )
        writer(reader)

        # check for broken symlinks
        import platform

        i_am_on_windows = platform.system() == "Windows"

        re_reader = UnderfolderReader(folder=writer_folder, copy_root_files=True)
        for sample in re_reader:
            for k, v in sample.filesmap.items():
                path = Path(v)
                assert i_am_on_windows or path.is_symlink()
                assert path.is_file()


class TestUnderfolderWriterFlushOnWrite(object):
    def test_flush_on_write(self, toy_dataset_small, tmpdir_factory):
        folder = toy_dataset_small["folder"]

        os.chdir(folder.parent)
        folder = folder.name

        reader = UnderfolderReader(folder=folder)
        writer_folder = Path(tmpdir_factory.mktemp(str(uuid.uuid1())))
        writer = UnderfolderWriter(
            folder=writer_folder, flush_on_write=True, num_workers=8
        )
        writer(reader)

        re_reader = UnderfolderReader(folder=writer_folder, copy_root_files=True)
        for sample, re_sample in zip(reader, re_reader):
            for k in sample:
                assert not sample.is_cached(k)
                assert not re_sample.is_cached(k)
                if isinstance(sample[k], np.ndarray):
                    assert np.allclose(sample[k], re_sample[k])
                else:
                    assert sample[k] == re_sample[k]


class TestUnderfolderLinkPlugin:
    def test_linking(self, tmpdir, plain_samples_sequence_generator):

        N = 32
        samples = plain_samples_sequence_generator("", N)

        # retrieve keys
        assert len(samples) > 0
        keys = list(samples[0].keys())

        # creates root keys as copy of normal keys
        root_keys = [f"{k}_root" for k in keys]
        for sample in samples:
            for rkey in root_keys:
                sample[rkey] = sample[rkey.replace("_root", "")]

        # Split datasets in N datasets for each key, write them to disk
        subsamples = {}
        writers = {}
        subfolders = {}
        for key, root_key in zip(keys, root_keys):
            subsamples[key] = OperationFilterKeys([key, root_key])(samples)
            subfolders[key] = Path(tmpdir.mkdir(key))
            writers[key] = UnderfolderWriter(
                folder=subfolders[key], root_files_keys=root_keys
            )
            writers[key](subsamples[key])
            rich.print(key, "Writing", subfolders[key])

        # Creates a tree to represent the linking structure among the subfolders
        g = nx.full_rary_tree(3, len(keys))
        for u, v, a in g.edges(data=True):
            key_source = keys[u]
            key_target = keys[v]
            folder_source = str(subfolders[key_source])
            folder_target = str(subfolders[key_target])
            UnderfolderLinksPlugin.link_underfolders(folder_source, folder_target)
            print(u, v, keys[u], keys[v])

        # Creates the reader of the Root Underfolder
        root_reader = UnderfolderReader(folder=subfolders[keys[0]])
        assert len(root_reader) > 0

        # Write the merged dataset for debug
        merged_folder = Path(tmpdir.mkdir("_merged"))
        writer = UnderfolderWriter(folder=merged_folder, root_files_keys=root_keys)
        writer(root_reader)
        rich.print("Merged output to", merged_folder)

        # Checks for keys/root_keys consistency
        for key in keys:
            assert not root_reader.is_root_key(key)
        for rkey in root_keys:
            assert root_reader.is_root_key(rkey)

        # Checks for merged keys presence
        ref_sample = root_reader[0]
        loaded_keys = set(list(ref_sample.keys()))
        assert loaded_keys == set(keys) | set(root_keys)

        # Content alignment test
        for sample in root_reader:
            assert int(sample["idx"]) == sample["number"]
            assert sample["odd"] == (sample["number"] % 2 == 1)
            assert np.isclose(sample["fraction"], sample["number"] / 1000.0)
            assert sample["reverse_number"] == N - sample["number"]
            assert sample["metadata"]["even"] == (sample["number"] % 2 == 0)
            assert sample["metadata"]["N"] == sample["number"]

        # Connect a Leaf to the root underfolder in order to build a cycle
        dg = nx.to_directed(g)
        leafs = [
            x for x in dg.nodes() if dg.out_degree(x) == 1 and dg.in_degree(x) == 1
        ]
        assert len(leafs) > 0
        folder_source = str(subfolders[keys[leafs[0]]])
        folder_target = str(subfolders[keys[0]])
        UnderfolderLinksPlugin.link_underfolders(folder_source, folder_target)

        # Checks for cycle when UnderfolderReader is called
        with pytest.raises(RuntimeError):
            UnderfolderReader(folder=subfolders[keys[0]])


class TestUnderfolderWriterV2(object):
    def _read_write_data(self, source_folder, **writer_kwargs):
        os.chdir(source_folder.parent)
        source_folder = source_folder.name

        reader = UnderfolderReader(folder=source_folder)
        writer = UnderfolderWriterV2(**writer_kwargs)
        writer(reader)

    def test_deep_copy(self, toy_dataset_small, tmpdir_factory):
        source_folder = toy_dataset_small["folder"]
        writer_folder = str(Path(tmpdir_factory.mktemp(str(uuid.uuid1()))))

        self._read_write_data(
            source_folder,
            folder=writer_folder,
            file_handling=UnderfolderWriterV2.FileHandling.ALWAYS_COPY_FROM_DISK,
            copy_mode=UnderfolderWriterV2.CopyMode.DEEP_COPY,
        )

        re_reader = UnderfolderReader(folder=writer_folder, copy_root_files=True)
        for sample in re_reader:
            for k, v in sample.filesmap.items():
                path = Path(v)
                assert not path.is_symlink()
                assert path.is_file()
                assert path.stat().st_nlink == 1

    def test_symlink(self, toy_dataset_small, tmpdir_factory):
        source_folder = toy_dataset_small["folder"]
        writer_folder = str(Path(tmpdir_factory.mktemp(str(uuid.uuid1()))))

        self._read_write_data(
            source_folder,
            folder=writer_folder,
            file_handling=UnderfolderWriterV2.FileHandling.ALWAYS_COPY_FROM_DISK,
            copy_mode=UnderfolderWriterV2.CopyMode.SYM_LINK,
        )

        # check for broken symlinks
        import platform

        i_am_on_windows = platform.system() == "Windows"

        re_reader = UnderfolderReader(folder=writer_folder, copy_root_files=True)
        for sample in re_reader:
            for k, v in sample.filesmap.items():
                path = Path(v)
                assert i_am_on_windows or path.is_symlink()
                assert path.is_file()
                assert path.stat().st_nlink == 1

    def test_hardlink(self, toy_dataset_small, tmpdir_factory):
        source_folder = toy_dataset_small["folder"]
        writer_folder = str(Path(tmpdir_factory.mktemp(str(uuid.uuid1()))))

        self._read_write_data(
            source_folder,
            folder=writer_folder,
            file_handling=UnderfolderWriterV2.FileHandling.ALWAYS_COPY_FROM_DISK,
            copy_mode=UnderfolderWriterV2.CopyMode.HARD_LINK,
        )

        re_reader = UnderfolderReader(folder=writer_folder, copy_root_files=True)
        for sample in re_reader:
            for k, v in sample.filesmap.items():
                path = Path(v)
                assert not path.is_symlink()
                assert path.is_file()
                assert path.stat().st_nlink == 2

    def test_always_write_from_cache(self, toy_dataset_small, tmpdir_factory):
        source_folder = toy_dataset_small["folder"]
        writer_folder = str(Path(tmpdir_factory.mktemp(str(uuid.uuid1()))))

        self._read_write_data(
            source_folder,
            folder=writer_folder,
            file_handling=UnderfolderWriterV2.FileHandling.ALWAYS_WRITE_FROM_CACHE,
            copy_mode=UnderfolderWriterV2.CopyMode.HARD_LINK,
        )

        re_reader = UnderfolderReader(folder=writer_folder, copy_root_files=True)
        for sample in re_reader:
            for k, v in sample.filesmap.items():
                path = Path(v)
                assert not path.is_symlink()
                assert path.is_file()
                assert path.stat().st_nlink == 1

    def test_copy_if_not_cached(self, toy_dataset_small, tmpdir_factory):
        source_folder = toy_dataset_small["folder"]
        writer_folder = str(Path(tmpdir_factory.mktemp(str(uuid.uuid1()))))

        self._read_write_data(
            source_folder,
            folder=writer_folder,
            file_handling=UnderfolderWriterV2.FileHandling.COPY_IF_NOT_CACHED,
            copy_mode=UnderfolderWriterV2.CopyMode.HARD_LINK,
        )

        re_reader = UnderfolderReader(folder=writer_folder, copy_root_files=True)
        for sample in re_reader:
            for k, v in sample.filesmap.items():
                path = Path(v)
                assert not path.is_symlink()
                assert path.is_file()
                assert path.stat().st_nlink == 2

    @pytest.mark.parametrize("num_threads", [0, 1, 2, 4, -1])
    def test_changed_ext(self, toy_dataset_small, tmpdir_factory, num_threads):
        source_folder = toy_dataset_small["folder"]
        os.chdir(source_folder.parent)
        source_folder = source_folder.name
        reader = UnderfolderReader(folder=source_folder)

        reader_template = reader.get_reader_template()
        changed_keys = []
        old_ext = "png"
        new_ext = "jpg"
        for k, ext in reader_template.extensions_map.items():
            if ext == old_ext:
                reader_template.extensions_map[k] = new_ext
                changed_keys.append(k)

        writer_folder = str(Path(tmpdir_factory.mktemp(str(uuid.uuid1()))))
        writer = UnderfolderWriterV2(
            folder=writer_folder,
            file_handling=UnderfolderWriterV2.FileHandling.ALWAYS_COPY_FROM_DISK,
            copy_mode=UnderfolderWriterV2.CopyMode.HARD_LINK,
            reader_template=reader_template,
            num_workers=num_threads,
        )
        writer(reader)

        for sample in reader:
            for k, v in sample.filesmap.items():
                path = Path(v)
                if k in changed_keys:
                    assert num_threads == -1 or num_threads > 1 or sample.is_cached(k)
                    assert not path.is_symlink()
                    assert path.is_file()
                    assert path.stat().st_nlink == 1
                else:
                    assert not sample.is_cached(k)
                    assert not path.is_symlink()
                    assert path.is_file()
                    assert path.stat().st_nlink == 2
