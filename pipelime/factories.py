
import typing
from abc import ABC, abstractmethod
from schema import Schema


class Bean(ABC):
    TYPE_FIELD = '__type__'
    ARGS_FIELD = 'options'

    @classmethod
    def bean_name(cls) -> str:
        return cls.__name__

    @classmethod
    @abstractmethod
    def bean_schema(cls) -> dict:
        pass

    @classmethod
    def full_bean_schema(cls) -> Schema:
        return Schema({
            Bean.TYPE_FIELD: cls.bean_name(),
            Bean.ARGS_FIELD: cls.bean_schema()
        })

    @classmethod
    def from_dict(cls, d: dict):
        return cls(**d)

    @abstractmethod
    def to_dict(self) -> dict:
        pass

    def serialize(self, validate: bool = True) -> dict:
        out = {
            Bean.TYPE_FIELD: self.bean_name(),
            Bean.ARGS_FIELD: self.to_dict()
        }
        if validate:
            self.full_bean_schema().validate(out)
        return out

    @classmethod
    def hydrate(cls, d: dict, validate: bool = True) -> any:
        if validate:
            cls.full_bean_schema().validate(d)
        return cls.from_dict(d[Bean.ARGS_FIELD])


class BeanFactory(object):

    CLASSES_MAP: typing.Dict[str, Bean] = {}

    @classmethod
    def register_bean(cls, x: Bean):
        cls.CLASSES_MAP[x.bean_name()] = x

    @classmethod
    def serialize(cls, x: Bean, validate: bool = True):
        return x.serialize(validate=validate)

    @classmethod
    def create(cls, d: dict, validate: bool = True):
        if d[Bean.TYPE_FIELD] in cls.CLASSES_MAP:
            bt = cls.CLASSES_MAP[d[Bean.TYPE_FIELD]]
            return bt.hydrate(d, validate=validate)
        raise RuntimeError(f'Non serializable data: {d}')

    @staticmethod
    def make_serializable(x: type):
        BeanFactory.register_bean(x)
        return x
