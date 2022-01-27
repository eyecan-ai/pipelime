from typing import Any
import numpy as np
from pydantic import BaseModel, validator
from pipelime.sequences.stages import SampleStage
from pipelime.sequences.samples import PlainSample, Sample, SamplesSequence
from pipelime.sequences.operations import OperationStage
import rich


class MetadataModel(BaseModel):
    a: bool
    b: float
    c: str

    @validator("c")
    def _verify_c(cls, value):
        if not value.startswith("hello"):
            raise ValueError("C must start with hello")


class SampleModel(BaseModel):
    image: Any
    metadata: MetadataModel
    sample_id: int

    @validator("image")
    def _verify_image(cls, value):
        if not isinstance(value, np.ndarray):
            raise ValueError("image must be a numpy array")


class StageVerify(SampleStage):
    class StageVerifyException(Exception):
        pass

    def __init__(self, sample_model: type):
        self._sample_model = sample_model

    def __call__(self, x: Sample) -> Sample:
        try:
            self._sample_model.parse_obj(x)
        except Exception as e:
            raise StageVerify.StageVerifyException(f"Sample ID: {x.id} -> {str(e)}")
        return x


class OperationVerify(OperationStage):
    def __init__(self, sample_model: type, **kwargs):
        super().__init__(stage=StageVerify(sample_model), **kwargs)


def main():
    samples = [
        PlainSample(
            {
                "sample_id": idx,
                "metadata": {
                    "a": True,
                    "b": 2.0,
                    "c": "hello",  # UNCOMMENT TO PASS VERIFICATION
                    # "c": "custom",  # UNCOMMENT TO INVALIDATE VERIFICATION
                },
                "image": np.random.uniform(0, 1, (256, 256, 3)),
            },
            id=idx,
        )
        for idx in range(100)
    ]
    samples = [PlainSample(x, id=x["sample_id"]) for x in samples]
    sequence = SamplesSequence(samples=samples)

    verify_op = OperationVerify(
        sample_model=SampleModel,
        num_workers=4,
        progress_bar=True,
    )

    try:
        verify_op(sequence)
        rich.print("[green] Verified! [/green]")
    except StageVerify.StageVerifyException as e:
        print("Validation failed!", e)


if __name__ == "__main__":
    main()
