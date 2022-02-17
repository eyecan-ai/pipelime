import pytest
from pipelime.pipes.parsers.factory import (
    DAGConfigParserFactory,
    DAGConfigParserFactoryConfigurationModel,
)
from pipelime.pipes.model import DAGModel
from choixe.configurations import XConfig
from deepdiff import DeepDiff


class TestDAGSimpleParser:
    def test_parsing(self, piper_dags: dict):

        for dag_name, item in piper_dags.items():

            folder = item["folder"]
            valid = item["valid"]
            exception = item["exception"]

            dag_to_parse = folder / "dag_to_parse.yml"
            dag_parsed = folder / "dag_parsed.yml"
            params = folder / "params.yml"

            # builds/validate the generic parser configuration
            cfg = DAGConfigParserFactoryConfigurationModel(
                **(XConfig(dag_to_parse).to_dict())
            )

            if valid:
                dag = DAGConfigParserFactory.parse_file(
                    cfg_file=dag_to_parse,
                    params_file=params if params.exists() else None,
                )

                assert isinstance(dag, DAGModel)

                if dag_parsed.exists():
                    parsed_dag = DAGModel(**(XConfig(filename=dag_parsed).to_dict()))

                    assert not DeepDiff(
                        dag.dict(),
                        parsed_dag.dict(),
                        ignore_order=True,
                    )
            else:

                with pytest.raises(exception):
                    DAGConfigParserFactory.parse_file(
                        cfg_file=dag_to_parse,
                        params_file=params if params.exists() else None,
                    )
