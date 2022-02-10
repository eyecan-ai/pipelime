from pipelime.sequences.readers.filesystem import UnderfolderReader
import numpy as np


class TestFSToolkit:
    def test_supported_data_types(self, toy_dataset_small):
        input_folder = toy_dataset_small["folder"]
        reader = UnderfolderReader(input_folder, copy_root_files=False)

        for x in reader:
            assert x["image"].shape == (256, 256, 3)
            assert x["mask"].shape == (256, 256)
            assert x["inst"].shape == (256, 256)

            assert x["metadata"] == x["metadataj"]
            # assert x["metadata"] == x["metadatat"]

            assert np.all(x["keypoints"] == x["keypointsp"])
            assert np.all(x["keypoints"] == x["metadata"]["keypoints"])

            assert np.all(x["bboxes"] == x["metadata"]["bboxes"])

            assert int.from_bytes(x["bin"], "big") == x["metadata"]["bin"]
