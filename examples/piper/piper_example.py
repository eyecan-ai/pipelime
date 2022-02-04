from choixe.configurations import XConfig
import click
import cv2
import rich
from pipelime.pipes.drawing.diagrams import DiagramsNodesGraphDrawer
from pipelime.pipes.executors import NaiveGraphExecutor
from pipelime.pipes.graph import NodesGraph
from pipelime.pipes.parser import PipesConfigParser


@click.command("piper_example")
@click.option("--piper_file", type=click.Path(exists=True), default="piper.yml")
@click.option("--piper_params_file", type=click.Path(exists=True), default="params.yml")
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

    # Create Parser
    parser = PipesConfigParser()

    # Nodes Model (abstraction layer to allow several parsing methods)
    nodes_model = parser.parse_cfg(
        cfg.to_dict(),
        global_data=global_data,
    )
    rich.print(nodes_model.dict())

    # Create graph from NodesModel
    graph = NodesGraph.build_nodes_graph(nodes_model)

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
        executor.exec(graph)


if __name__ == "__main__":
    piper_example()
