import click

from pipelime.cli.workflow.run import run
from pipelime.cli.workflow.click2cwl import click2cwl
from pipelime.cli.workflow.show_graph import show_graph
from pipelime.cli.workflow.list_nodes import list_nodes


@click.group()
def workflow():
    pass


workflow.add_command(run)
workflow.add_command(click2cwl)
workflow.add_command(show_graph)
workflow.add_command(list_nodes)
