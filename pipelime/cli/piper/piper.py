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
    from choixe.configurations import XConfig

    from pipelime.pipes.parsers.factory import DAGConfigParserFactory
    from pipelime.tools.click import ClickTools

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

    import cv2
    import numpy as np
    import rich

    from pipelime.filesystem.toolkit import FSToolkit
    from pipelime.pipes.drawing.factory import NodesGraphDrawerFactory
    from pipelime.pipes.graph import DAGNodesGraph
    from pipelime.pipes.parsers.factory import DAGConfigParserFactory
    from pipelime.tools.click import ClickTools

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
@click.option("-i", "--piper_file", type=click.Path(exists=True), default="dag.yml")
@click.option("-p", "--piper_params_file", type=click.Path(), default="params.yml")
@click.option(
    "-b", "--execution_backend", type=click.Choice(["naive"]), default="naive"
)
@click.option("-t", "--token", type=str, default="")
@click.option("--watch", is_flag=True, help="Live printing of tasks progress.")
@click.pass_context
def execute(
    ctx: click.Context,
    piper_file: str,
    piper_params_file: str,
    execution_backend: str,
    token: str,
    watch: bool,
):

    from pipelime.pipes.executors.base import WatcherNodesGraphExecutor
    from pipelime.pipes.executors.naive import NaiveGraphExecutor
    from pipelime.pipes.communication import PiperCommunicationChannelFactory
    from pipelime.pipes.graph import DAGNodesGraph
    from pipelime.pipes.parsers.factory import DAGConfigParserFactory
    from pipelime.tools.click import ClickTools

    # Backend selection
    if execution_backend == "naive":
        executor = NaiveGraphExecutor()
    else:
        raise NotImplementedError(
            f"Execution backend {execution_backend} not implemented"
        )

    # Decorators
    if watch:
        executor = WatcherNodesGraphExecutor(executor)

    # DAG Model (abstraction layer to allow several parsing methods)
    dag = DAGConfigParserFactory.parse_file(
        cfg_file=piper_file,
        params_file=piper_params_file,
        additional_args=ClickTools.parse_additional_args(ctx),  # additional args
    )
    # Graph
    graph = DAGNodesGraph.build_nodes_graph(dag)
    executor.exec(graph, token=token)

    PiperCommunicationChannelFactory.create_channel(token).close()


if __name__ == "__main__":
    piper()
