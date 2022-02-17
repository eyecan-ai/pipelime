from typing import Callable
from pytest import TempPathFactory
import pytest
from pipelime.pipes.parsers.factory import DAGConfigParserFactory
from pipelime.pipes.graph import DAGNodesGraph
from pipelime.pipes.executors.naive import NaiveGraphExecutor
from pipelime.tools.modules import ModulesUtils
from choixe.configurations import XConfig


class TestNaiveGraphExecutor:
    def test_executor(self, piper_dags: dict, tmp_path_factory: TempPathFactory):

        something_checked_control = False

        for dag_name, item in piper_dags.items():
            folder = item["folder"]
            valid = item.get("valid", False)
            graph = item.get("graph", False)
            executable = item.get("executable", {})

            executable_success = executable.get("success", False)
            executable_exception = executable.get("exception", None)
            executable_placeholders = executable.get("executable_placeholders", {})

            dag_to_parse = folder / "dag_to_parse.yml"

            if executable and valid and graph:
                something_checked_control = True  # PARANOID!

                run_folder = tmp_path_factory.mktemp("run")

                # Load params with Placeholders
                params_with_placeholders = folder / "params_with_placeholders.yml"
                assert params_with_placeholders.exists()
                cfg = XConfig(params_with_placeholders)

                # Replace params placeholders with values provided by user in conftest
                cfg.replace_variables_map(executable_placeholders)
                assert len(cfg.available_placeholders()) == 0

                # write parsed params to file
                writed_params_file = run_folder / "params.yml"
                cfg.save_to(writed_params_file)

                # Load Dag with parsed params
                dag = DAGConfigParserFactory.parse_file(
                    cfg_file=dag_to_parse,
                    params_file=writed_params_file,
                )

                # Build graph from NodesModel
                graph = DAGNodesGraph.build_nodes_graph(dag_model=dag)

                if executable_success:

                    # Execute graph
                    executor = NaiveGraphExecutor()
                    executor.exec(graph, token="")

                    # CHecks for FINAL VALIDATION file
                    final_validation_file = folder / "final_validation.py"
                    if final_validation_file.exists():

                        # Extract final validation function
                        func: Callable = ModulesUtils.load_variable_from_file(
                            str(final_validation_file),
                            "final_validation",
                        )
                        func(**executable_placeholders)

                else:
                    with pytest.raises(executable_exception):
                        # Execute graph
                        executor = NaiveGraphExecutor()
                        executor.exec(graph, token="")

        assert something_checked_control

    def test_executor_with_additional_arguments(
        self, piper_dags: dict, tmp_path_factory: TempPathFactory
    ):

        something_checked_control = False

        for dag_name, item in piper_dags.items():
            folder = item["folder"]
            valid = item.get("valid", False)
            graph = item.get("graph", False)
            executable = item.get("executable", {})

            executable_success = executable.get("success", False)
            executable_exception = executable.get("exception", None)
            executable_placeholders = executable.get("executable_placeholders", {})

            dag_to_parse = folder / "dag_to_parse.yml"

            if executable and valid and graph:
                something_checked_control = True  # PARANOID!

                run_folder = tmp_path_factory.mktemp("run")

                # Load params with Placeholders
                params_with_placeholders = folder / "params_with_placeholders.yml"

                # Load Dag with parsed params
                dag = DAGConfigParserFactory.parse_file(
                    cfg_file=dag_to_parse,
                    params_file=params_with_placeholders,
                    additional_args=executable_placeholders,
                )

                # Build graph from NodesModel
                graph = DAGNodesGraph.build_nodes_graph(dag_model=dag)

                if executable_success:

                    # Execute graph
                    executor = NaiveGraphExecutor()
                    executor.exec(graph, token="")

                    # CHecks for FINAL VALIDATION file
                    final_validation_file = folder / "final_validation.py"
                    if final_validation_file.exists():

                        # Extract final validation function
                        func: Callable = ModulesUtils.load_variable_from_file(
                            str(final_validation_file),
                            "final_validation",
                        )
                        func(**executable_placeholders)

                else:
                    with pytest.raises(executable_exception):
                        # Execute graph
                        executor = NaiveGraphExecutor()
                        executor.exec(graph, token="")

        assert something_checked_control
