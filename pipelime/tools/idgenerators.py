import uuid
from abc import ABC, abstractmethod
from itertools import count

from choixe.spooks import Spook


class IdGenerator(ABC):
    @abstractmethod
    def generate(self):
        pass


class IdGeneratorInteger(IdGenerator, Spook):
    def __init__(self) -> None:
        super().__init__()
        self.COUNTER = count()

    def generate(self):
        return next(self.COUNTER)

    def to_dict(self) -> dict:
        return {}


class IdGeneratorUUID(IdGenerator, Spook):
    def generate(self):
        return str(uuid.uuid1())
