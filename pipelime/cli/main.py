import click

from pipelime.cli.datasets.datasets import datasets
from pipelime.cli.workflow.workflow import workflow
from pipelime.cli.conversions.conversions import conversions
from pipelime.cli.underfolder.underfolder import underfolder
from pipelime.cli.h5.h5 import h5


@click.group()
def pipelime():
    pass


pipelime.add_command(datasets)
pipelime.add_command(workflow)
pipelime.add_command(conversions)
pipelime.add_command(underfolder)
pipelime.add_command(h5)
