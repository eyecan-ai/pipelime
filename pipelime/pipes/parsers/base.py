from abc import ABC, abstractclassmethod
from typing import Optional
from pipelime.pipes.model import DAGModel
from choixe.configurations import XConfig


class DAGConfigParser(ABC):
    @abstractclassmethod
    def parse_cfg(self, cfg: dict, global_data: Optional[dict] = None) -> DAGModel:
        """Parses the given configuration into a DAGModel.

        Args:
            cfg (dict): input configuration as dictionary
            global_data (Optional[dict], optional): the global data dictionary. Defaults to None.

        Returns:
            DAGModel: resulting DAGModel
        """
        pass

    def parse_file(self, cfg_file: str, params_file: Optional[str] = None) -> DAGModel:
        """Parses the given configuration file into a DAGModel.

        Args:
            cfg_file (str): input configuration file
            params_file (Optional[str], optional): the global data file. Defaults to None.

        Returns:
            DAGModel: resulting DAGModel
        """

        cfg = XConfig(cfg_file).to_dict()
        global_data = None
        if params_file:
            global_data = XConfig(params_file).to_dict()
        return self.parse_cfg(cfg=cfg, global_data=global_data)
