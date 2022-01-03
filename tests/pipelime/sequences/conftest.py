import numpy as np
import pytest

from pipelime.sequences.samples import PlainSample, SamplesSequence


@pytest.fixture(scope="session")
def plain_samples_generator():
    def generate_plain_samples(
        namespace: str, N: int, group_each: int = 5, heavy_data: bool = True
    ):

        samples = []
        for i in range(N):
            data = {
                "idx": f"{namespace}{i}",
                "number": i,
                "reverse_number": N - i,
                "fraction": i / 1000.0,
                "odd": i % 2 == 1,
                "data0": np.random.uniform(0.0, 1.0, (64, 64, 3))
                if heavy_data
                else 1.64643,
                "data1": np.random.uniform(0, 255, (8, 8)).astype(np.uint8)
                if heavy_data
                else 25588,
                "metadata": {
                    "even": i % 2 == 0,
                    "name": f"{namespace}{i}",
                    "N": i,
                    "deep": {"super_deep": 0, "groupby_field": int(i / group_each)},
                },
            }
            samples.append(PlainSample(data=data, id=i))

        return samples

    return generate_plain_samples


@pytest.fixture(scope="session")
def plain_samples_sequence_generator(plain_samples_generator):
    def generate_plain_samples_sequence(
        namespace: str, N: int, group_each: int = 5, heavy_data: bool = True
    ):
        return SamplesSequence(
            samples=plain_samples_generator(namespace, N, group_each, heavy_data)
        )

    return generate_plain_samples_sequence
