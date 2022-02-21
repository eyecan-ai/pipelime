from pathlib import Path
import imageio
from pipelime.sequences.operations import OperationResetIndices
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.samples import FileSystemSample, Sample
from pipelime.sequences.streams.base import ItemConverter
from pipelime.sequences.streams.underfolder import UnderfolderStream
import io
import pytest
import shutil
import numpy as np

from pipelime.sequences.writers.filesystem import UnderfolderWriter
from pipelime.tools.idgenerators import IdGeneratorUUID


class TestUnderfolderStreams:
    def _create_dataset(sample_underfolder_minimnist, tmp_path):
        folder = tmp_path / "dataset"
        shutil.copytree(sample_underfolder_minimnist["folder"], folder)
        return folder

    def test_stream_empty(self, sample_underfolder_empty):

        view = UnderfolderStream(sample_underfolder_empty["folder"])
        assert len(view) == 0

        assert view.manifest()["size"] == 0

    def test_stream_read(self, sample_underfolder_minimnist, tmp_path):
        folder = sample_underfolder_minimnist["folder"]

        dataset = UnderfolderReader(folder=folder)
        view = UnderfolderStream(folder)
        assert len(view) > 0
        assert len(dataset) == len(view)

        manifest = view.manifest()
        assert manifest is not None
        assert isinstance(manifest, dict)
        assert "keys" in manifest
        keys = manifest["keys"]

        for sample_id in range(len(view)):

            sample = view.get_sample(sample_id)
            assert isinstance(sample, Sample)

            for key in keys:
                assert key in sample

                if key == "image":
                    for format in ItemConverter.IMAGE_FORMATS:
                        image, mimetype = view.get_data(sample_id, key, format=format)
                        assert isinstance(image, io.BytesIO)

                    with pytest.raises(ValueError):
                        view.get_data(sample_id, key, format="invalid")
                    with pytest.raises(ValueError):
                        ItemConverter.item_to_data(sample[key], "invalid")

                elif key == "label" or key == "points":
                    for format in ItemConverter.MATRIX_FORMATS:
                        label, mimetype = view.get_data(sample_id, key, format=format)
                        assert isinstance(label, dict)
                        assert "data" in label

                elif key == "metadata" or key == "metadatay":
                    for format in ItemConverter.DICT_FORMATS:
                        metadata, mimetype = view.get_data(
                            sample_id, key, format=format
                        )
                        assert isinstance(metadata, dict)

            key = "IMPOSSIBLE_KEY!"
            assert key not in sample

            with pytest.raises(KeyError):
                view.get_item(sample_id, key)

    def test_stream_write(self, sample_underfolder_minimnist, tmp_path):
        folder = tmp_path / "dataset"
        shutil.copytree(sample_underfolder_minimnist["folder"], folder)
        print("folder:", folder)
        dataset = UnderfolderReader(folder=folder)
        view = UnderfolderStream(folder)
        assert len(view) > 0
        assert len(dataset) == len(view)

        for sample_id in view.get_sample_ids():

            view.set_data(
                sample_id, "new_metadata", {"data": [1, 2, 3.0], "flag": True}, "dict"
            )
            view.set_data(sample_id, "new_matrix", {"data": [1, 2, 3.0]}, "matrix")
            view.set_data(
                sample_id,
                "new_points",
                {"data": [1, 2, 3.0, 4, 5, 6]},
                "matrix",
            )

            image = np.random.rand(28, 28, 3)
            image_bytes = io.BytesIO()
            imageio.imwrite(image_bytes, image, format="jpg")
            view.set_data(sample_id, "new_image", image_bytes, "jpg")

        view.flush()
        for sample in view.reader:
            assert isinstance(sample, FileSystemSample)
            for key in sample:
                assert not sample.is_cached(key)

        dataset = UnderfolderReader(folder=folder)
        for sample in dataset:
            assert "new_metadata" in sample
            assert "new_matrix" in sample
            assert "new_points" in sample
            assert "new_image" in sample

    def test_stream_write_uuid(self, sample_underfolder_minimnist, tmp_path):
        temp_reader = UnderfolderReader(sample_underfolder_minimnist["folder"])
        filtered = OperationResetIndices(generator=IdGeneratorUUID())(temp_reader)

        folder = tmp_path / "dataset"
        UnderfolderWriter(
            folder=folder,
            extensions_map=temp_reader.get_reader_template().extensions_map,
            root_files_keys=temp_reader.get_reader_template().root_files_keys,
        )(filtered)

        dataset = UnderfolderReader(folder=folder)
        view = UnderfolderStream(folder)
        assert len(view) > 0
        assert len(dataset) == len(view)

        for sample_id in view.get_sample_ids():

            view.set_data(
                sample_id, "new_metadata", {"data": [1, 2, 3.0], "flag": True}, "dict"
            )
            view.set_data(sample_id, "new_matrix", {"data": [1, 2, 3.0]}, "matrix")
            view.set_data(
                sample_id,
                "new_points",
                {"data": [1, 2, 3.0, 4, 5, 6]},
                "matrix",
            )

            image = np.random.rand(28, 28, 3)
            image_bytes = io.BytesIO()
            imageio.imwrite(image_bytes, image, format="jpg")
            view.set_data(sample_id, "new_image", image_bytes, "jpg")

        view.flush()
        for sample in view.reader:
            assert isinstance(sample, FileSystemSample)
            for key in sample:
                assert not sample.is_cached(key)

        dataset = UnderfolderReader(folder=folder)
        for sample in dataset:
            assert "new_metadata" in sample
            assert "new_matrix" in sample
            assert "new_points" in sample
            assert "new_image" in sample

    def test_stream_notallowed_keys(self, sample_underfolder_minimnist, tmp_path):
        folder = tmp_path / "dataset"
        shutil.copytree(sample_underfolder_minimnist["folder"], folder)

        allowed_keys = ["image", "points"]
        view = UnderfolderStream(folder, allowed_keys=allowed_keys)
        assert len(view) > 0

        for sample_id in range(len(view)):

            with pytest.raises(PermissionError):
                view.set_data(
                    sample_id, "metadata", {"data": [1, 2, 3.0], "flag": True}, "dict"
                )

            with pytest.raises(PermissionError):
                view.set_data(sample_id, "matrix", {"data": [1, 2, 3.0]}, "matrix")

            view.set_data(
                sample_id,
                "points",
                {"data": [1, 2, 3.0, 4, 5, 6]},
                "matrix",
            )

            image = np.random.rand(28, 28, 3)
            image_bytes = io.BytesIO()
            imageio.imwrite(image_bytes, image, format="jpg")
            view.set_data(sample_id, "image", image_bytes, "jpg")

    def test_stream_write_format(self, sample_underfolder_minimnist, tmp_path):
        folder = tmp_path / "dataset"
        shutil.copytree(sample_underfolder_minimnist["folder"], folder)
        print("folder:", folder)
        view = UnderfolderStream(folder)

        ##############################
        # Write without further specification means DEFAULT FILE EXTENSION
        ##############################
        for sample_id in range(len(view)):
            view.set_data(
                sample_id, "new_metadata", {"data": [1, 2, 3.0], "flag": True}, "dict"
            )

        dataset = UnderfolderReader(folder=folder)
        for sample in dataset:
            assert isinstance(sample, FileSystemSample)
            filepath = Path(sample.filesmap["new_metadata"])
            default_extension = UnderfolderWriter.DEFAULT_EXTENSION
            assert filepath.suffix.replace(".", "") == default_extension

        ##############################
        # Write with specification, overwriting default extension multiple times
        ##############################
        for file_format in ["json", "yml", "pkl"]:
            view.add_extensions_map({"last_metadata": file_format})
            for sample_id in range(len(view)):
                view.set_data(
                    sample_id,
                    "last_metadata",
                    {"data": [1, 2, 3.0], "flag": True},
                    "dict",
                )

            dataset = UnderfolderReader(folder=folder)
            for sample in dataset:
                assert "last_metadata" in sample
                assert isinstance(sample, FileSystemSample)
                filepath = Path(sample.filesmap["last_metadata"])
                assert filepath.suffix.replace(".", "") == file_format
                assert sample["last_metadata"]["flag"]

    def test_stream_write_rootkeys(self, sample_underfolder_minimnist, tmp_path):
        folder = tmp_path / "dataset"
        shutil.copytree(sample_underfolder_minimnist["folder"], folder)
        print("folder:", folder)
        view = UnderfolderStream(folder)

        view.add_root_files_keys(["new_metadata"])
        ##############################
        # Write without further specification means DEFAULT FILE EXTENSION
        ##############################
        for sample_id in range(len(view)):
            view.set_data(
                sample_id, "new_metadata", {"data": [1, 2, 3.0], "flag": True}, "dict"
            )
            break

        dataset = UnderfolderReader(folder=folder)
        for sample in dataset:
            assert "new_metadata" in sample
