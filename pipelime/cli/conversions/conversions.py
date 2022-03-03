import click

from pipelime.cli.conversions.h5_to_underfolder import h5_to_underfolder
from pipelime.cli.conversions.underfolder_to_h5 import underfolder_to_h5
from pipelime.cli.conversions.subfolders_to_underfolder import subfolders_to_underfolder
from pipelime.cli.conversions.smart_converter import smart_converter


@click.group()
def conversions():
    pass


conversions.add_command(underfolder_to_h5)
conversions.add_command(h5_to_underfolder)
conversions.add_command(subfolders_to_underfolder)
conversions.add_command(smart_converter)
