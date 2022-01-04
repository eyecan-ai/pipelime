from pipelime.sequences.readers.filesystem import (
    UnderfolderReader,
    UnderfolderStagePlugin,
)
from pipelime.sequences.samples import Sample
from pipelime.sequences.stages import SampleStage
import rich


class MyStageEquation(SampleStage):
    def __init__(self, a: float, b: float):
        self._a = a
        self._b = b

    def __call__(self, x: Sample) -> Sample:
        x["result"] = x["value"] * self._a + self._b
        return x

    @classmethod
    def spook_schema(cls) -> dict:
        return {"a": float, "b": float}

    @classmethod
    def from_dict(cls, d: dict):
        return MyStageEquation(a=d["a"], b=d["b"])

    def to_dict(self):
        return {"a": self._a, "b": self._b}


# create the stage
stage = MyStageEquation(a=2.0, b=3.0)

# inject the stage to the dataset in 'dataset' folder
UnderfolderStagePlugin.set_stages("dataset", stage)

###############################################################################
# Stages are automatically applied to the dataset samples when retreived

# read the dataset (with stage)
reader = UnderfolderReader("dataset")

for sample in reader:
    rich.print("Sample:", sample.id)
    rich.print("\t", "Original Value:", sample["value"])
    rich.print("\t", "Transformed Value:", sample["result"])
