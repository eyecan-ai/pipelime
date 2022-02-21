import io
from pipelime.sequences.streams.underfolder import UnderfolderStream
import rich
import tempfile
import cv2
import shutil
from pathlib import Path
import imageio
import numpy as np

input_folder = "../../../tests/sample_data/datasets/underfolder_minimnist/"
output_folder = Path(tempfile.mkdtemp()) / "dataset"
shutil.copytree(input_folder, output_folder)

stream = UnderfolderStream(folder=output_folder)

for sample_id in range(len(stream)):

    # RESIZE IMAGE AND PUSH
    # fetch image
    data, _ = stream.get_data(sample_id, "image", "jpg")
    image = imageio.imread(data)

    # resize image and conver to bytes
    image: np.ndarray = cv2.resize(image, (256, 256))
    image_bytes = io.BytesIO()
    imageio.imwrite(image_bytes, image, format="jpg")

    # push image
    stream.set_data(sample_id, "image", image_bytes, "jpg")

    # UPDATE METADATA
    # fetch metadata
    metadata, _ = stream.get_data(sample_id, "metadata", "dict")

    # update metadata
    metadata["updated"] = True
    metadata["resize"] = 256

    # push metadata
    stream.set_data(sample_id, "metadata", metadata, "dict")


rich.print("[red]Output written to:[/red]", output_folder)
