import pytest
from pipelime.pipes.parsers.factory import DAGConfigParserFactory
from pipelime.pipes.graph import DAGNodesGraph


from pytest import TempPathFactory


class TestDrawDAGNodesGraph:
    def test_draw_graph_creation(
        self, piper_dags: dict, tmp_path_factory: TempPathFactory
    ):

        something_checked_control = False

        try:
            from pipelime.pipes.drawing.factory import NodesGraphDrawerFactory
        except ImportError as e:
            pytest.skip(f"Backend not installed: {e}")

        backends = NodesGraphDrawerFactory.available_backends()

        for dag_name, item in piper_dags.items():

            folder = item["folder"]
            valid = item.get("valid", False)
            graph = item.get("graph", False)

            dag_to_parse = folder / "dag_to_parse.yml"
            params = folder / "params.yml"

            if valid and graph:
                something_checked_control = True
                dag = DAGConfigParserFactory.parse_file(
                    cfg_file=dag_to_parse,
                    params_file=params if params.exists() else None,
                )

                graph = DAGNodesGraph.build_nodes_graph(dag_model=dag)

                for backend in backends:
                    drawer_folder = tmp_path_factory.mktemp(backend)
                    drawer = NodesGraphDrawerFactory.create(backend)
                    formats = drawer.exportable_formats()
                    for format in formats:
                        filename = drawer_folder / f"{dag_name}.{format}"
                        print(backend, drawer_folder, filename)
                        drawer.draw(graph)
                        drawer.representation(graph)
                        drawer.export(graph, filename, format)
                        drawer.export(graph, filename, None)

        assert something_checked_control
