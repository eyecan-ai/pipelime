from choixe.configurations import XConfig
import click
import cv2
import rich
from pipelime.pipes.drawing.diagrams import DiagramsNodesGraphDrawer
from pipelime.pipes.executors.naive import NaiveGraphExecutor
from pipelime.pipes.graph import DAGNodesGraph
from pipelime.pipes.parsers.factory import DAGConfigParserFactory


@click.command("piper_example")
@click.option(
    "--piper_file",
    type=click.Path(exists=True),
    default="../../tests/sample_data/piper/dags/complex/dag_to_parse.yml",
)
@click.option(
    "--piper_params_file",
    type=click.Path(exists=True),
    default="../../tests/sample_data/piper/dags/complex/params.yml",
)
@click.option("--draw/--execute", default=True, type=bool)
@click.option("--draw_output", default="", type=click.Path())
def piper_example(
    piper_file: str,
    piper_params_file: str,
    draw: bool,
    draw_output: str,
):

    # Load configuration
    cfg = XConfig(piper_file)

    # Load global data
    global_data = XConfig(piper_params_file).to_dict()

    # DAG Model (abstraction layer to allow several parsing methods)
    dag = DAGConfigParserFactory.parse_file(
        cfg_file=piper_file,
        params_file=piper_params_file,
    )

    rich.print(dag.dict())
    XConfig.from_dict(dag.purged_dict()).save_to("dag_enrolled.yml")

    # Create graph from NodesModel
    graph = DAGNodesGraph.build_nodes_graph(dag)

    if draw:
        # Drawer
        drawer = DiagramsNodesGraphDrawer()
        img = drawer.draw(graph)
        cv2.imshow("graph", cv2.cvtColor(img, cv2.COLOR_RGB2BGR))
        if len(draw_output) == 0:
            cv2.waitKey(0)
        else:
            cv2.imwrite(draw_output, cv2.cvtColor(img, cv2.COLOR_RGB2BGR))
            rich.print("graph image saved to:", draw_output)
    else:
        # Executor
        executor = NaiveGraphExecutor()
        executor.exec(graph, token="miao")


if __name__ == "__main__":
    piper_example()
