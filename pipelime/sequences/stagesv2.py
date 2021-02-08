import rich
import typing
import serpyco
import albumentations as A
from abc import ABC, abstractmethod
from typing import Dict, Hashable, Sequence, Union

from schema import Schema
from pipelime.sequences.samples import Sample
import dataclasses


@dataclasses.dataclass
class XSerializable(object):
    __type__: str = ''

    @classmethod
    @serpyco.post_dump
    def make_type(cls, data: dict) -> dict:
        data['__type__'] = cls.__name__
        return data


class Metropolis(object):

    CLASSES_MAP = {}

    @classmethod
    def register_class(cls, x: type):
        cls.CLASSES_MAP[x.__name__] = serpyco.Serializer(x)

    @classmethod
    def serialize(cls, x: XSerializable):
        _name = x.__class__.__name__
        assert _name in cls.CLASSES_MAP
        return cls.CLASSES_MAP[_name].dump(x)

    @classmethod
    def hydrate(cls, d: dict, validate: bool = True):
        assert '__type__' in d
        assert d['__type__'] in cls.CLASSES_MAP
        return cls.CLASSES_MAP[d['__type__']].load(d, validate=validate)

    @staticmethod
    def factorizable(x: type):
        Metropolis.register_class(x)
        return x


class XSampleStage(ABC, XSerializable):

    @abstractmethod
    def __call__(self, x:
                 Union[
                     Sample,
                     Sequence[Sample],
                     Dict[Hashable, Sample]
                 ]) -> Union[
                     Sample,
                     Sequence[Sample],
                     Dict[Hashable, Sample]
    ]:
        pass


@Metropolis.factorizable
@dataclasses.dataclass
class XStageIdentity(XSampleStage):

    def __call__(self, x: any) -> any:
        return x


@dataclasses.dataclass
class MyField(object):
    x: float = 1.0
    y: float = 2.0


@Metropolis.factorizable
@dataclasses.dataclass
class XStageRemap(XSampleStage):
    remap: Dict[str, str] = None
    remove_missing: bool = False
    o: typing.List[MyField] = dataclasses.field(default_factory=list)

    def __call__(self, x: Sample) -> Sample:

        out: Sample = x.copy()
        for k in x.keys():
            # for k, v in x.items():
            if k in self._remap:
                out.rename(k, self._remap[k])
            else:
                if self._remove_missing:
                    del out[k]
        return out


# s = XStageRemap(remap={'a': 'b'}, remove_missing=False, o=[MyField(), MyField()])

# d = Metropolis.serialize(s)
# rich.print(d)

# ss = Metropolis.hydrate(d)
# rich.print(ss)

# serializer = serpyco.Serializer(XStageRemap)
# d = serializer.dump(s)
# d['ciao'] = 'miao'
# rich.print(d)

# obj = serializer.load(d, validate=True)

# rich.print(obj)


# s2 = XStageIdentity()
# serializer = serpyco.Serializer(XStageIdentity)

# rich.print(serializer.dump(s2))

def foo(x: type):
    x = dataclasses.dataclass(x)
    print("foo", x, type(x))
    return x


@foo
class AAA(XSampleStage):
    remap: Dict[str, str] = None
    remove_missing: bool = False
    o: typing.List[MyField] = dataclasses.field(default_factory=list)

    def __call__(self, x: Sample) -> Sample:

        out: Sample = x.copy()
        for k in x.keys():
            # for k, v in x.items():
            if k in self._remap:
                out.rename(k, self._remap[k])
            else:
                if self._remove_missing:
                    del out[k]
        return out


a = AAA()
rich.print(AAA.__dict__)
