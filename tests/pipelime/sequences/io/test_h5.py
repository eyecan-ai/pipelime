from pathlib import Path

import h5py
import numpy as np
import pytest
from choixe.spooks import Spook

from pipelime.sequences.readers.base import BaseReader
from pipelime.sequences.readers.h5 import H5Item, H5Reader, H5Sample
from pipelime.sequences.samples import Sample
from pipelime.sequences.writers.base import BaseWriter
from pipelime.sequences.writers.h5 import H5Writer


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


def _plug_test_writer(writer: BaseWriter):
    """Test what a generic BaseWriter should do

    :param writer: input BaseWriter
    :type writer: BaseWriter
    """

    assert isinstance(writer, BaseWriter)

    print(writer.serialize())
    rewriter = writer.hydrate(writer.serialize())
    assert isinstance(rewriter, BaseWriter)

    factored = Spook.create(writer.serialize())
    assert isinstance(factored, BaseWriter)


class TestH5(object):
    def test_reader(self, h5_datasets: dict, tmpdir):

        output_folder = Path(tmpdir.mkdir("test_h5reader_writer"))

        for dataset_name, cfg in h5_datasets.items():
            filename = cfg["filename"]
            reader = H5Reader(filename=filename, copy_root_files=True)

            _plug_test(reader)
            assert len(reader) > 0
            for sample in reader:
                assert isinstance(sample, Sample)
                assert isinstance(sample, H5Sample)
                assert len(sample.keys()) > 0

            out_filename = output_folder / (dataset_name + ".h5")
            writer = H5Writer(filename=out_filename)
            _plug_test_writer(writer)
            writer(reader)

            second_reader = H5Reader(filename=out_filename, copy_root_files=True)
            assert len(second_reader) == len(reader)
            assert len(second_reader) > 0

            proto_sample = second_reader[0]
            assert len(proto_sample.keys()) > 0
            for sample in second_reader:
                for key in sample:
                    assert key in proto_sample

    def test_reader_copy_root(self, h5_datasets: dict, tmpdir):

        for dataset_name, cfg in h5_datasets.items():
            filename = cfg["filename"]
            reader_copy = H5Reader(filename=filename, copy_root_files=True)
            reader_nocopy = H5Reader(filename=filename, copy_root_files=False)

            assert len(reader_copy) == len(reader_nocopy)

            for sample_copy, sample_nocopy in zip(reader_copy, reader_nocopy):
                assert len(sample_copy.keys()) > len(sample_nocopy.keys())


class TestH5Sample(object):
    def test_h5_sample(self, h5_datasets):

        for dataset_name, cfg in h5_datasets.items():
            filename = cfg["filename"]
            reader = H5Reader(filename=filename, copy_root_files=True)

            sample = reader[0]

            for key in sample.keys():
                assert isinstance(sample.metaitem(key), H5Item)
                item: H5Item = sample.metaitem(key)
                assert isinstance(item.source(), h5py.Dataset)

            assert len(sample) > 0
            assert isinstance(sample["image"], np.ndarray)
            assert isinstance(sample, H5Sample)

            with pytest.raises(NotImplementedError):
                sample.rename("image", "_NEW_IMAGE_")

            del sample["image"]

    def test_flush(self, h5_datasets):

        for dataset_name, cfg in h5_datasets.items():
            filename = cfg["filename"]
            reader = H5Reader(filename=filename, copy_root_files=True)

            temp = None
            for sample in reader:
                sample: H5Sample
                for key in sample.keys():
                    temp = sample[key]
                    assert temp is not None
                    assert sample.is_cached(key)

            reader.flush()

            for sample in reader:
                sample: H5Sample
                for key in sample.keys():
                    assert not sample.is_cached(key)

    def test_copy(self, h5_datasets):

        for dataset_name, cfg in h5_datasets.items():
            filename = cfg["filename"]
            reader = H5Reader(filename=filename, copy_root_files=True)

            for sample in reader:
                sample: H5Sample

                keys = list(sample.keys())[:2]
                for key in keys:
                    temp = sample[key]
                    assert temp is not None
                    assert sample.is_cached(key)

                sample_copy = sample.copy()
                assert isinstance(sample_copy, type(sample))

                for key in sample.keys():
                    assert sample.is_cached(key) == sample_copy.is_cached(key)

    def test_skeleton(self, h5_datasets):

        for dataset_name, cfg in h5_datasets.items():
            filename = cfg["filename"]
            reader = H5Reader(filename=filename, copy_root_files=True)

            for sample in reader:
                sample: H5Sample
                assert sample.skeleton.keys() == sample.keys()
                for key in sample.skeleton.keys():
                    assert sample.skeleton[key] is None

    def test_nonlazy(self, h5_datasets: dict, tmpdir):

        for dataset_name, cfg in h5_datasets.items():
            filename = cfg["filename"]
            reader = H5Reader(
                filename=filename, lazy_samples=False, copy_root_files=True
            )

            for sample in reader:
                assert isinstance(sample, H5Sample)
                for key in sample.keys():
                    assert sample.is_cached(key)
                    assert isinstance(sample.metaitem(key), H5Item)
