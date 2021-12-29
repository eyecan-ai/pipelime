import io

import yaml
from pipelime.sequences.readers.filesystem import UnderfolderReader
import rich
import tempfile
import cv2
import shutil
from pathlib import Path
import imageio
import numpy as np
from pipelime.tools.toydataset import ToyDatasetGenerator

names = ["A", "B", "C"]
size = 20
base_folder = Path(tempfile.mkdtemp())
output_folders = {x: base_folder / x for x in names}


# Generates Toy Dataset and relative Readers
readers = {}
for name, folder in output_folders.items():
    ToyDatasetGenerator.generate_toy_dataset(
        folder, size, suffix=f"_{name}", as_undefolder=True
    )
    readers[name] = UnderfolderReader(folder)

# Write links to the last reader
last_reader = readers[names[-1]]
last_folder = output_folders[names[-1]]
UnderfolderReader.link_underfolders(output_folders["B"], str(output_folders["A"]))
UnderfolderReader.link_underfolders(output_folders["C"], str(output_folders["B"]))

rich.print("Output folders")
rich.print([str(x) for x in output_folders.values()])

# Last reader has links to the first two folders
linked_reader = UnderfolderReader(last_folder)
for sample in linked_reader:
    rich.print("Merged keys")
    rich.print(list(sample.keys()))
    break
