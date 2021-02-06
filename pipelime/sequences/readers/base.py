from pipelime.factories import Factorizable
from pipelime.sequences.samples import FileSystemSample, SamplesSequence
from abc import abstractmethod
from schema import Schema


class BaseReader(SamplesSequence, Factorizable):

    pass
