import click

from pipelime.cli.conversions.h5_to_underfolder import h5_to_underfolder
from pipelime.cli.conversions.underfolder_to_h5 import underfolder_to_h5


@click.group()
def conversions():
    pass


conversions.add_command(underfolder_to_h5)
conversions.add_command(h5_to_underfolder)
