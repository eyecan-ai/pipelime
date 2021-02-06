
from choixe.configurations import XConfig
from schema import Optional, Schema
import albumentations as A


class AugmentationsFactory(object):

    AUGMENTATIONS_CFG_SCHEMA = Schema({
        Optional('__version__'): str,
        'transform': dict
    })

    @classmethod
    def build_from_file(cls, filename: str):
        cfg = XConfig(filename=filename)
        return cls.build_from_dict(cfg.to_dict())

    @classmethod
    def build_from_dict(cls, cfg: dict):
        cfg = XConfig.from_dict(cfg)
        cfg.set_schema(cls.AUGMENTATIONS_CFG_SCHEMA)
        cfg.validate()
        return A.from_dict(cfg.to_dict(discard_private_qualifiers=False))
