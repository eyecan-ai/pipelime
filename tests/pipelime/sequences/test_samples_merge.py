from pathlib import Path
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.readers.h5 import H5Reader
from pipelime.sequences.samples import GroupedSample, PlainSample, SamplesSequence
from pipelime.sequences.writers.filesystem import UnderfolderWriter
from pipelime.sequences.writers.h5 import H5Writer
from pipelime.tools.toydataset import ToyDatasetGenerator
import rich
import functools
import shutil


class TestSampleMergesGroupedSample:
    def test_merge(self, plain_samples_generator):

        dataset = plain_samples_generator("d0_", 10)

        g0 = GroupedSample(samples=dataset[:5])
        g1 = GroupedSample(samples=dataset[5:])

        for k in ["added_0", "added_1"]:
            g1[k] = "new_value"

        merged = g0.merge(g1)
        assert set(merged.keys()) == set(g0.keys()) | set(g1.keys())


class TestSampleMergesPlainSample:
    def test_merge(self):

        keys = ["A", "B", "C"]
        plain_samples = [PlainSample({key: 1.0}) for key in keys]
        merged_sample = functools.reduce(lambda x, y: x.merge(y), plain_samples)
        assert set(merged_sample.keys()) == set(keys)


class TestSampleMergesUnderfolder:
    def test_merges(self, tmpdir):

        # Builds Folders and Filenames
        size = 32
        sub_datasets = ["A", "B", "C"]
        root_folders = [Path(tmpdir.mkdir(x)) for x in sub_datasets]
        data_folders = [x / UnderfolderReader.DATA_SUBFOLDER for x in root_folders]
        rich.print("Folders:")
        [rich.print(x) for x in root_folders]

        # Generates Toy Dataset
        for name, folder in zip(sub_datasets, data_folders):
            ToyDatasetGenerator.generate_toy_dataset(folder, size, suffix=name)

        # Builds Reader for each toy dataset
        readers = {x: UnderfolderReader(y) for x, y in zip(sub_datasets, root_folders)}

        # Adds an overlapping key to each sample in each dataset
        for reader_name, reader in readers.items():
            for sample in reader:
                sample["added_key"] = reader_name

        # computes the set of merged keys (account for overlapping keys)
        keys_set = set()
        for reader_name, reader in readers.items():
            assert len(reader) == size
            assert len(reader) > 0
            keys_set.update(reader[0].keys())

        # merge all the datasets
        merged_reader = SamplesSequence.merge_sequences(readers.values())

        assert len(merged_reader) == size
        assert len(merged_reader) > 0

        # checks that all the keys are present
        ref_sample = merged_reader[0]
        assert len(keys_set) > 0
        assert len(ref_sample.keys()) == len(keys_set)

        # Checks if overlapping keys values are set equal to the last sample in merging lists
        assert ref_sample["added_key"] == sub_datasets[-1]

        # write merged dataset to underfolder
        merged_folder = Path(tmpdir.mkdir("merged"))
        writer = UnderfolderWriter(folder=merged_folder)
        writer(merged_reader)
        rich.print("Output written to:", merged_folder)

        # read merged dataset from underfolder
        r_reader = UnderfolderReader(folder=merged_folder)
        assert len(r_reader) > 0
        assert len(r_reader) == len(merged_reader)


class TestSampleMergesH5:
    def test_merges(self, tmpdir):

        # Builds Folders and Filenames
        size = 32
        sub_datasets = ["A", "B", "C"]
        root_folders = [Path(tmpdir.mkdir(x)) for x in sub_datasets]
        data_folders = [x / UnderfolderReader.DATA_SUBFOLDER for x in root_folders]
        h5_files = [x / "dataset.h5" for x in root_folders]

        rich.print("Files:")
        [rich.print(x) for x in root_folders]

        # Generate toy data
        for name, folder in zip(sub_datasets, data_folders):
            ToyDatasetGenerator.generate_toy_dataset(folder, size, suffix=name)

        # Underfolder readers for toy datasets
        readers = {x: UnderfolderReader(y) for x, y in zip(sub_datasets, root_folders)}

        # H5Writer for each dataset
        writers = {x: H5Writer(y) for x, y in zip(sub_datasets, h5_files)}
        for name, reader in readers.items():
            writer = writers[name]
            writer(reader)

        # Removed unused underfolder reader of toy datasets
        for d in data_folders:
            shutil.rmtree(d)

        # Reader for h5 toy data
        readers = {x: H5Reader(y) for x, y in zip(sub_datasets, h5_files)}

        # Checks for size of each dataset
        for reader_name, reader in readers.items():
            assert len(reader) == size

        # Merge datasets
        merged_reader = SamplesSequence.merge_sequences(readers.values())

        assert len(merged_reader) == size
        assert len(merged_reader) > 0

        # Adds an overlapping key to each sample in each dataset
        for reader_name, reader in readers.items():
            for sample in reader:
                sample["added_key"] = reader_name

        # Builds a set of merged keys (account for overlapping keys)
        keys_set = set()
        for reader_name, reader in readers.items():
            assert len(reader) == size
            assert len(reader) > 0
            keys_set.update(reader[0].keys())

        ref_sample = merged_reader[0]
        assert len(keys_set) > 0
        assert len(ref_sample.keys()) == len(keys_set)

        # Write merged dataset to h5 file
        merged_h5_file = Path(tmpdir.mkdir("merged")) / "dataset.h5"
        writer = H5Writer(filename=merged_h5_file)
        writer(merged_reader)
        rich.print("Output written to:", merged_h5_file)

        # Read merged dataset from h5 file
        r_reader = H5Reader(filename=merged_h5_file)
        assert len(r_reader) > 0
