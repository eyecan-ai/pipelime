
from abc import ABC, abstractmethod
from itertools import count
from pipelime.factories import Bean, BeanFactory
import uuid


class IdGenerator(ABC):

    @abstractmethod
    def generate(self):
        pass


@BeanFactory.make_serializable
class IdGeneratorInteger(IdGenerator, Bean):

    def __init__(self) -> None:
        super().__init__()
        self.COUNTER = count()   

    def generate(self):
        return next(self.COUNTER)


@BeanFactory.make_serializable
class IdGeneratorUUID(IdGenerator, Bean):

    def generate(self):
        return str(uuid.uuid1())
