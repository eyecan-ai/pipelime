import click

from pipelime.cli.h5.operations import summary


@click.group()
def h5():
    pass


h5.add_command(summary)
