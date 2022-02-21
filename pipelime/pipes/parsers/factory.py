from typing import Optional, Sequence
from pydantic import BaseModel
from pipelime.pipes.parsers.base import DAGConfigParser
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
    def parse_file(
        cls,
        cfg_file: str,
        params_file: Optional[str] = None,
        additional_args: Optional[dict] = None,
    ) -> DAGModel:
        """Parse the given configuration file into a DAGModel.

        Args:
            cfg_file (str): input configuration file
            params_file (Optional[str], optional): additional params file. Defaults to None.
            additional_args (Optional[dict], optional): _description_. Defaults to None.

        Raises:
            NotImplementedError: _description_

        Returns:
            DAGModel: _description_
        """

        # builds/validate the generic parser configuration
        cfg = DAGConfigParserFactoryConfigurationModel(**(XConfig(cfg_file).to_dict()))

        # is parser available?
        if cfg.parser_name in cls.FACTORY_MAP:

            # retrieve the parser
            parser: DAGConfigParser = cls.FACTORY_MAP[cfg.parser_name]()

            return parser.parse_file(
                cfg_file=cfg_file,
                params_file=params_file,
                additional_args=additional_args,
            )
        else:
            raise NotImplementedError(f"parser {cfg.parser_name} not implemented")

    @classmethod
    def available_parsers(cls) -> Sequence[str]:
        return list(DAGConfigParserFactory.FACTORY_MAP.keys())
