import click

from pipelime.cli.workflow.run import run
from pipelime.cli.workflow.click2cwl import click2cwl
from pipelime.cli.workflow.show_graph import show_graph
from pipelime.cli.workflow.list_nodes import list_nodes
from pipelime.cli.workflow.delete_nodes import delete_nodes
from pipelime.cli.workflow.update_nodes import update_nodes
from pipelime.cli.workflow.init_workflow import init


@click.group()
def workflow():
    pass


workflow.add_command(run)
workflow.add_command(click2cwl)
workflow.add_command(show_graph)
workflow.add_command(list_nodes)
workflow.add_command(delete_nodes)
workflow.add_command(update_nodes)
workflow.add_command(init)
