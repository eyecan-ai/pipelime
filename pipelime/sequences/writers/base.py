from pipelime.factories import Bean
from pipelime.sequences.samples import SamplesSequence
from abc import abstractmethod


class BaseWriter(Bean):

    @abstractmethod
    def __call__(self, x: SamplesSequence) -> None:
        pass
