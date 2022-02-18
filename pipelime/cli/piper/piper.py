from calendar import c
from typing import Optional
import click


@click.group()
def piper():
    pass


@piper.command(
    "compile",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.option(
    "-i",
    "--piper_file",
    type=click.Path(exists=True),
    default="dag.yml",
)
@click.option(
    "-p",
    "--piper_params_file",
    type=click.Path(),
    default="params.yml",
)
@click.option(
    "-o",
    "--output_file",
    type=click.Path(),
    default="",
)
@click.pass_context
def compile(
    ctx: click.Context,
    piper_file: str,
    piper_params_file: str,
    output_file: str,
):

    import rich
    from pipelime.tools.click import ClickTools
    from choixe.configurations import XConfig
    import rich
    from pipelime.pipes.parsers.factory import DAGConfigParserFactory

    # DAG Model (abstraction layer to allow several parsing methods)
    try:
        dag = DAGConfigParserFactory.parse_file(
            cfg_file=piper_file,
            params_file=piper_params_file,
            additional_args=ClickTools.parse_additional_args(ctx),  # additional args
        )
    except KeyError as e:
        raise click.UsageError(f"Something is missing! -> {e}")

    cfg = XConfig.from_dict(dag.purged_dict())

    if len(output_file) > 0:
        cfg.save_to(output_file)
        rich.print("Saved to:", output_file)
    else:
        rich.print(cfg.to_dict())


@piper.command(
    "draw",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.option(
    "-i",
    "--piper_file",
    type=click.Path(exists=True),
    default="dag.yml",
)
@click.option(
    "-p",
    "--piper_params_file",
    type=click.Path(exists=True),
    default="params.yml",
)
@click.option(
    "-b",
    "--draw_backend",
    # TODO: don't use factory list to avoid missing dependencies check on Workflows
    type=click.Choice(["graphviz", "mermaid"]),
    default="graphviz",
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
@click.pass_context
def draw(
    ctx: click.Context,
    piper_file: str,
    piper_params_file: str,
    draw_backend: str,
    output_file: str,
    open: bool,
):

    from pipelime.pipes.drawing.factory import NodesGraphDrawerFactory
    from pipelime.pipes.parsers.factory import DAGConfigParserFactory
    from pipelime.filesystem.toolkit import FSToolkit
    from pipelime.pipes.graph import DAGNodesGraph
    from pipelime.tools.click import ClickTools
    import numpy as np
    import rich
    import cv2

    # DAG Model (abstraction layer to allow several parsing methods)
    dag = DAGConfigParserFactory.parse_file(
        cfg_file=piper_file,
        params_file=piper_params_file,
        additional_args=ClickTools.parse_additional_args(ctx),  # additional args
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


@piper.command(
    "execute",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.option(
    "-i",
    "--piper_file",
    type=click.Path(exists=True),
    default="dag.yml",
)
@click.option(
    "-p",
    "--piper_params_file",
    type=click.Path(),
    default="params.yml",
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
@click.pass_context
def execute(
    ctx: click.Context,
    piper_file: str,
    piper_params_file: str,
    execution_backend: str,
    token: str,
):

    from pipelime.pipes.parsers.factory import DAGConfigParserFactory
    from pipelime.pipes.executors.naive import NaiveGraphExecutor
    from pipelime.pipes.graph import DAGNodesGraph
    from pipelime.tools.click import ClickTools

    # DAG Model (abstraction layer to allow several parsing methods)
    dag = DAGConfigParserFactory.parse_file(
        cfg_file=piper_file,
        params_file=piper_params_file,
        additional_args=ClickTools.parse_additional_args(ctx),  # additional args
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
