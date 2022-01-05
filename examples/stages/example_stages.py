from choixe.spooks import Spook
from choixe.configurations import XConfig

from pipelime.sequences.readers.filesystem import UnderfolderReader

stage = Spook.create(XConfig("stages.yml"))
print(stage)


dataset = UnderfolderReader("../data/underfolder/example_dataset")
# dataset.stage = stage

for sample in dataset:
    print(list(sample.keys()))
