from abc import ABC, abstractclassmethod
from typing import Optional
from pipelime.pipes.model import DAGModel
from choixe.configurations import XConfig


class DAGConfigParser(ABC):
    PARAMS_NAMESPACE = "params"
    NODES_NAMESPACE = "nodes"

    @abstractclassmethod
    def parse_cfg(
        self,
        cfg: dict,
        global_data: Optional[dict] = None,
    ) -> DAGModel:
        """Parses the given configuration into a DAGModel.

        Args:
            cfg (dict): input configuration as dictionary
            global_data (Optional[dict], optional): the global data dictionary. Defaults to None.

        Returns:
            DAGModel: resulting DAGModel
        """
        pass

    def parse_file(
        self,
        cfg_file: str,
        params_file: Optional[str] = None,
        additional_args: Optional[dict] = None,
    ) -> DAGModel:
        """Parses the given configuration file into a DAGModel.

        Args:
            cfg_file (str): input configuration file
            params_file (Optional[str], optional): the global data file. Defaults to None.

        Returns:
            DAGModel: resulting DAGModel
        """

        cfg = XConfig(cfg_file).to_dict()
        global_data = {}
        if params_file:
            global_data = XConfig(params_file)

            if additional_args is not None:

                # Replace placeholders filling default values if not provided
                global_data.replace_variables_map(
                    additional_args,
                    replace_defaults=True,
                )

            # Checks for placeholders, if any close the execution!
            global_data.check_available_placeholders(close_app=True)

            global_data = global_data.to_dict()

        return self.parse_cfg(cfg=cfg, global_data=global_data)
