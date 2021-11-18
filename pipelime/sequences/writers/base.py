from choixe.spooks import Spook
from pipelime.sequences.samples import SamplesSequence
from abc import abstractmethod


class BaseWriter(Spook):
    @abstractmethod
    def __call__(self, x: SamplesSequence) -> None:
        pass
