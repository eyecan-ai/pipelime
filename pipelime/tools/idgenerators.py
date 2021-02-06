
from abc import ABC, abstractmethod
from itertools import count
import uuid


class IdGenerator(ABC):

    @classmethod
    @abstractmethod
    def generate(cls):
        pass


class IdGeneratorInteger(IdGenerator):
    COUNTER = count()

    @classmethod
    def generate(cls):
        return next(cls.COUNTER)


class IdGeneratorUUID(IdGenerator):

    @classmethod
    def generate(cls):
        return str(uuid.uuid1())
