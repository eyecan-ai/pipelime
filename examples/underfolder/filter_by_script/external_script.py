from pipelime.sequences.samples import Sample, SamplesSequence
import numpy as np

MIN_ANGLE = 8.0


def check_sample(x: Sample, s: SamplesSequence) -> bool:
    pose = x['pose']
    z = pose[:3, 2].reshape((3, ))
    gravity = np.array([0, 0, -1.]).reshape((3,))
    angle = 180 * np.arccos(np.dot(z, gravity)) / np.pi
    return angle >= MIN_ANGLE
