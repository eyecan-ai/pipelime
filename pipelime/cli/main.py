import click

from pipelime.cli.datasets.datasets import datasets
from pipelime.cli.workflow.workflow import workflow
from pipelime.cli.underfolder.underfolder import underfolder


@click.group()
def pipelime():
    pass


pipelime.add_command(datasets)
pipelime.add_command(workflow)
pipelime.add_command(underfolder)
