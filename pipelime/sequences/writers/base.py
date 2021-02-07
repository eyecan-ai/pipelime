from pipelime.factories import Factorizable
from pipelime.sequences.samples import FileSystemSample, SamplesSequence
from abc import abstractmethod
from schema import Schema


class BaseWriter(Factorizable):

    @abstractmethod
    def __call__(self, x: SamplesSequence) -> None:
        pass
