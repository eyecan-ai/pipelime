from typing import Optional
import click


@click.group()
def piper():
    pass


@piper.command("compile")
@click.option(
    "-i",
    "--piper_file",
    type=click.Path(exists=True),
    required=True,
)
@click.option(
    "-p",
    "--piper_params_file",
    type=click.Path(),
    default="",
)
@click.option(
    "-o",
    "--output_file",
    type=click.Path(),
    default="",
)
def compile(piper_file: str, piper_params_file: str, output_file: str):

    from choixe.configurations import XConfig
    import rich
    from pipelime.pipes.parsers.factory import DAGConfigParserFactory

    # DAG Model (abstraction layer to allow several parsing methods)
    try:
        dag = DAGConfigParserFactory.parse_file(
            cfg_file=piper_file,
            params_file=piper_params_file,
        )
    except KeyError as e:
        raise click.UsageError(f"Something is missing! -> {e}")

    cfg = XConfig.from_dict(dag.purged_dict())

    if len(output_file) > 0:
        cfg.save_to(output_file)
        rich.print("Saved to:", output_file)
    else:
        rich.print(cfg.to_dict())


@piper.command("draw")
@click.option(
    "-i",
    "--piper_file",
    type=click.Path(exists=True),
    required=True,
)
@click.option(
    "-p",
    "--piper_params_file",
    type=click.Path(),
    default="",
)
@click.option(
    "-b",
    "--draw_backend",
    type=click.Choice(["diagrams"]),
    default="diagrams",
)
@click.option(
    "-o",
    "--output_file",
    type=click.Path(),
    default="",
)
def draw(piper_file: str, piper_params_file: str, draw_backend: str, output_file: str):

    from pipelime.pipes.drawing.diagrams import DiagramsNodesGraphDrawer
    from pipelime.pipes.parsers.factory import DAGConfigParserFactory
    from pipelime.pipes.graph import DAGNodesGraph
    import numpy as np
    import rich
    import cv2

    # DAG Model (abstraction layer to allow several parsing methods)
    dag = DAGConfigParserFactory.parse_file(
        cfg_file=piper_file,
        params_file=piper_params_file,
    )

    # Graph
    graph = DAGNodesGraph.build_nodes_graph(dag)

    # Drawn image
    graph_image: Optional[np.ndarray] = None

    # Draw with selected backend
    if draw_backend == "diagrams":
        drawer = DiagramsNodesGraphDrawer()
        graph_image = drawer.draw(graph=graph)
    else:
        raise NotImplementedError(f"Drawing backend {draw_backend} not implemented")

    # Show or Write
    if len(output_file) > 0:
        cv2.imwrite(output_file, cv2.cvtColor(graph_image, cv2.COLOR_RGB2BGR))
        rich.print("graph image saved to:", output_file)
    else:
        cv2.imshow("graph", cv2.cvtColor(graph_image, cv2.COLOR_RGB2BGR))
        cv2.waitKey(0)


@piper.command("execute")
@click.option(
    "-i",
    "--piper_file",
    type=click.Path(exists=True),
    required=True,
)
@click.option(
    "-p",
    "--piper_params_file",
    type=click.Path(),
    default="",
)
@click.option(
    "-b",
    "--execution_backend",
    type=click.Choice(["naive"]),
    default="naive",
)
@click.option(
    "-t",
    "--token",
    type=str,
    default="",
)
def execute(
    piper_file: str, piper_params_file: str, execution_backend: str, token: str
):

    from pipelime.pipes.drawing.diagrams import DiagramsNodesGraphDrawer
    from pipelime.pipes.parsers.factory import DAGConfigParserFactory
    from pipelime.pipes.executors.naive import NaiveGraphExecutor
    from pipelime.pipes.graph import DAGNodesGraph

    # DAG Model (abstraction layer to allow several parsing methods)
    dag = DAGConfigParserFactory.parse_file(
        cfg_file=piper_file,
        params_file=piper_params_file,
    )

    # Graph
    graph = DAGNodesGraph.build_nodes_graph(dag)

    if execution_backend == "naive":
        executor = NaiveGraphExecutor()
        executor.exec(graph, token=token)
    else:
        raise NotImplementedError(
            f"Execution backend {execution_backend} not implemented"
        )


if __name__ == "__main__":
    piper()
