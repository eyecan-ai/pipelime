from choixe.spooks import Spook
import rich
from pipelime.sequences.readers.filesystem import UnderfolderReader

# load stages from file
stage = Spook.create_from_file("stages.yml")

# Input dataset
dataset = UnderfolderReader("../data/underfolder/example_dataset")

# Before stage
rich.print("Before stage:")
for sample in dataset:
    rich.print(list(sample.keys()))

# After stage
rich.print("After stage:")
dataset.stage = stage
for sample in dataset:
    rich.print(list(sample.keys()))
