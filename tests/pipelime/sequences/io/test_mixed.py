from pathlib import Path
from pipelime.sequences.writers.h5 import H5Writer
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.readers.h5 import H5Reader
from pipelime.sequences.writers.filesystem import UnderfolderWriter


class TestH5Underfolder:
    def test_cascade_huh(self, h5_datasets: dict, tmpdir):

        output_folder = Path(tmpdir.mkdir("test_h5reader_huh"))

        for dataset_name, cfg in h5_datasets.items():
            filename = cfg["filename"]
            h5_reader = H5Reader(filename=filename, copy_root_files=True)

            for sample in h5_reader:
                for key in sample.keys():
                    assert sample[key] is not None

            underfolder_0 = output_folder / "0"

            underfolder_writer = UnderfolderWriter(folder=underfolder_0)
            underfolder_writer(h5_reader)
            underfolder_reader = UnderfolderReader(
                folder=underfolder_0, copy_root_files=True
            )

            assert len(underfolder_reader) == len(h5_reader)
            assert len(underfolder_reader) > 0

            for idx in range(len(underfolder_reader)):
                sample_h5 = h5_reader[idx]
                sample_underfolder = underfolder_reader[idx]
                assert len(sample_h5.keys()) > 0
                assert len(sample_h5.keys()) == len(sample_underfolder.keys())
                for key in sample_h5.keys():
                    assert key in sample_underfolder
                for key in sample_underfolder.keys():
                    assert key in sample_h5

                for key in sample_h5.keys():
                    assert h5_reader.is_root_key(key) == underfolder_reader.is_root_key(
                        key
                    )

            h5filename_0 = output_folder / "h5.py"
            h5_writer = H5Writer(filename=h5filename_0)

            h5_writer(underfolder_reader)

            last_reader_h5 = H5Reader(filename=h5filename_0)

            assert len(last_reader_h5) > 0
            assert len(last_reader_h5) == len(h5_reader)

            for sample in last_reader_h5:
                for key in sample.keys():
                    assert sample[key] is not None

    def test_cascade_uhu(self, toy_dataset_small: dict, tmpdir):

        output_folder = Path(tmpdir.mkdir("test_h5reader_uhu"))

        folder = toy_dataset_small["folder"]
        keys = toy_dataset_small["expected_keys"]
        root_keys = toy_dataset_small["root_keys"]
        print(keys, root_keys)

        underfolder_reader = UnderfolderReader(folder=folder, copy_root_files=True)

        h5_0 = output_folder / "h5py"

        h5_writer = H5Writer(filename=h5_0)
        h5_writer(underfolder_reader)

        h5_reader = H5Reader(filename=h5_0)

        underfolder_0 = output_folder / "0"
        underfolder_writer = UnderfolderWriter(folder=underfolder_0)
        underfolder_writer(h5_reader)
