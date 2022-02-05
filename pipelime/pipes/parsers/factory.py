from typing import Optional
from pydantic import BaseModel
from pipelime.pipes.parsers.simple import DAGSimpleParser
from pipelime.pipes.model import DAGModel
from choixe.configurations import XConfig


class DAGConfigParserFactoryConfigurationModel(BaseModel):
    parser_name: str = "DAGSimpleParser"


class DAGConfigParserFactory:

    FACTORY_MAP = {
        "DAGSimpleParser": DAGSimpleParser,
    }

    @classmethod
    def parse_file(cls, cfg_file: str, params_file: Optional[str] = None) -> DAGModel:
        cfg = DAGConfigParserFactoryConfigurationModel(**(XConfig(cfg_file).to_dict()))
        if cfg.parser_name in cls.FACTORY_MAP:
            return cls.FACTORY_MAP[cfg.parser_name]().parse_file(
                cfg_file=cfg_file,
                params_file=params_file,
            )
        else:
            raise NotImplementedError(f"parser {cfg.parser_name} not implemented")
