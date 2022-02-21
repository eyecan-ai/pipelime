import rich
from pipelime.pipes.parsers.factory import DAGConfigParserFactory
from pipelime.pipes.graph import DAGNodesGraph
import networkx as nx


class TestDAGNodesGraph:
    def test_graph_creation(self, piper_dags: dict):

        something_checked_control = False

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
                assert isinstance(graph, DAGNodesGraph)
                assert isinstance(graph.raw_graph, nx.DiGraph)
                assert len(graph.root_nodes) > 0

                assert len(graph.operations_graph.raw_graph.nodes()) < len(
                    graph.raw_graph.nodes()
                )
                assert len(graph.data_graph.raw_graph.nodes()) < len(
                    graph.raw_graph.nodes()
                )

                execution_stack = graph.build_execution_stack()
                assert len(execution_stack) > 0

                nodes_to_execute = []
                for layer in execution_stack:
                    rich.print(layer)
                    rich.print([x for x in layer])
                    nodes_to_execute.extend([x for x in layer])

                assert len(nodes_to_execute) == len(
                    graph.operations_graph.raw_graph.nodes()
                )
                for nodetoexec in nodes_to_execute:
                    assert nodetoexec in graph.operations_graph.raw_graph.nodes()

        assert something_checked_control
