import click

from pipelime.cli.underfolder.operations import (
    dump,
    operation_filterbyquery,
    operation_filterbyscript,
    operation_filterkeys,
    operation_flatten,
    operation_groupby,
    operation_mix,
    operation_orderby,
    operation_remap_keys,
    operation_repeat,
    operation_shuffle,
    operation_split,
    operation_split_by_value,
    operation_splitbyquery,
    operation_subsample,
    operation_sum,
    remove_remote,
    summary,
    upload_to_remote,
    operation_remove_duplicates,
)


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
underfolder.add_command(operation_mix)
underfolder.add_command(operation_remap_keys)
underfolder.add_command(operation_flatten)
underfolder.add_command(operation_repeat)
underfolder.add_command(summary)
underfolder.add_command(upload_to_remote)
underfolder.add_command(remove_remote)
underfolder.add_command(dump)
underfolder.add_command(operation_remove_duplicates)
