from typing import Optional
import click

from pipelime.pipes.drawing.factory import NodesGraphDrawerFactory


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
    type=click.Choice(NodesGraphDrawerFactory.available_backends()),
    default="diagrams",
)
@click.option(
    "-o",
    "--output_file",
    type=click.Path(),
    default="",
)
@click.option(
    "--open/--no-open",
    type=bool,
    default=False,
    help="If TRUE, Open the output with corresponding default app",
)
def draw(
    piper_file: str,
    piper_params_file: str,
    draw_backend: str,
    output_file: str,
    open: bool,
):

    from pipelime.pipes.parsers.factory import DAGConfigParserFactory
    from pipelime.filesystem.toolkit import FSToolkit
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
    drawer = NodesGraphDrawerFactory.create(draw_backend)

    # Show or Write
    if len(output_file) > 0:
        drawer.export(graph, output_file)
        if open:
            FSToolkit.start_file(output_file)
        else:
            rich.print("graph image saved to:", output_file)
    else:
        graph_image = drawer.draw(graph=graph)
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
