from abc import abstractmethod

from choixe.spooks import Spook

from pipelime.sequences.samples import SamplesSequence


class BaseWriter(Spook):
    @abstractmethod
    def __call__(self, x: SamplesSequence) -> None:
        pass
