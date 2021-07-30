import click

from pipelime.cli.underfolder.operations import operation_sum
from pipelime.cli.underfolder.operations import operation_subsample
from pipelime.cli.underfolder.operations import operation_shuffle
from pipelime.cli.underfolder.operations import operation_split
from pipelime.cli.underfolder.operations import operation_filterbyquery
from pipelime.cli.underfolder.operations import operation_splitbyquery
from pipelime.cli.underfolder.operations import operation_filterbyscript
from pipelime.cli.underfolder.operations import operation_filterkeys
from pipelime.cli.underfolder.operations import operation_orderby
from pipelime.cli.underfolder.operations import operation_split_by_value
from pipelime.cli.underfolder.operations import operation_groupby


@click.group()
def underfolder():
    pass


underfolder.add_command(operation_sum)
underfolder.add_command(operation_subsample)
underfolder.add_command(operation_shuffle)
underfolder.add_command(operation_split)
underfolder.add_command(operation_filterbyquery)
underfolder.add_command(operation_splitbyquery)
underfolder.add_command(operation_filterbyscript)
underfolder.add_command(operation_filterkeys)
underfolder.add_command(operation_orderby)
underfolder.add_command(operation_split_by_value)
underfolder.add_command(operation_groupby)
