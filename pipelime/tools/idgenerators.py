
from abc import ABC, abstractmethod
from itertools import count
from pipelime.factories import Bean, BeanFactory
import uuid


class IdGenerator(ABC):

    @classmethod
    @abstractmethod
    def generate(cls):
        pass


@BeanFactory.make_serializable
class IdGeneratorInteger(IdGenerator, Bean):
    COUNTER = count()

    @classmethod
    def generate(cls):
        return next(cls.COUNTER)


@BeanFactory.make_serializable
class IdGeneratorUUID(IdGenerator, Bean):

    @classmethod
    def generate(cls):
        return str(uuid.uuid1())
