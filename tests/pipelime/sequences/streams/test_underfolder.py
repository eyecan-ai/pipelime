import imageio
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.samples import Sample
from pipelime.sequences.streams.base import ItemConverter
from pipelime.sequences.streams.underfolder import UnderfolderStream
import io
import pytest
import shutil
import numpy as np


class TestDatasetView:
    def test_datasetview_empty(self, sample_underfolder_empty):

        view = UnderfolderStream(sample_underfolder_empty['folder'])
        assert len(view) == 0

        with pytest.raises(ValueError):
            manifest = view.manifest()

    def test_datasetview(self, sample_underfolder_minimnist, tmp_path):
        folder = sample_underfolder_minimnist['folder']

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

            with pytest.raises(ValueError):
                view.get_item(sample_id, key)

    def test_datasetview_write(self, sample_underfolder_minimnist, tmp_path):
        folder = tmp_path / "dataset"
        shutil.copytree(sample_underfolder_minimnist['folder'], folder)
        print("folder:", folder)
        dataset = UnderfolderReader(folder=folder)
        view = UnderfolderStream(folder)
        assert len(view) > 0
        assert len(dataset) == len(view)

        for sample_id in range(len(view)):

            view.set_data(
                sample_id, "new_metadata", {"data": [1, 2, 3.0], "gino": True}, "dict"
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
            imageio.imwrite(image_bytes, image, format='jpg')
            view.set_data(sample_id, "new_image", image_bytes, "jpg")

        view.flush()

        dataset = UnderfolderReader(folder=folder)
        for sample in dataset:
            assert "new_metadata" in sample
            assert "new_matrix" in sample
            assert "new_points" in sample
            assert "new_image" in sample
