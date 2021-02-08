from pipelime.factories import Bean, Factorizable
from pipelime.sequences.samples import FileSystemSample, SamplesSequence
from abc import abstractmethod
from schema import Schema


class BaseReader(SamplesSequence, Bean):

    pass
