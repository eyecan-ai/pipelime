
from abc import ABC, abstractmethod
from typing import Dict
from schema import Schema


class Factorizable(ABC):

    @classmethod
    @abstractmethod
    def factory_name(cls) -> str:
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def factory_schema(cls) -> Schema:
        raise NotImplementedError()

    @classmethod
    def build_from_dict(cls, d: dict):
        cls.factory_schema().validate(d)

    @abstractmethod
    def to_dict(self) -> dict:
        pass


class GenericFactory(object):

    FACTORY_MAP: Dict[str, Factorizable] = {}

    @classmethod
    def generic_schema(cls) -> Schema:
        return Schema({
            'type': str,
            object: object
        })

    @classmethod
    def register_class(cls, x: object):
        cls.FACTORY_MAP[x.__name__] = x

    @classmethod
    def create(cls, cfg: dict) -> object:
        cls.generic_schema().validate(cfg)
        _t = cls.FACTORY_MAP[cfg['type']]
        return _t.build_from_dict(cfg)

    @staticmethod
    def register(o: object) -> object:
        GenericFactory.register_class(o)
        return o
