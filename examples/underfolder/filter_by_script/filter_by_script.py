import click
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.writers.filesystem import UnderfolderWriter
from pipelime.sequences.operations import OperationFilterByScript
import rich


@click.command('filter_by_script', help='Filter UnderfolderReader by external script')
@click.option('-d', '--dataset', required=True, type=str, help='Input Dataset')
@click.option('-o', '--output', required=True, type=str, help='Output Dataset folder')
@click.option('-s', '--script', default='external_script.py', type=str, help='Filtering Script')
def filter_by_script(dataset, output, script):

    reader = UnderfolderReader(folder=dataset)
    op = OperationFilterByScript(path_or_func=script)

    reader_template = reader.get_filesystem_template()

    writer = UnderfolderWriter(
        folder=output,
        root_files_keys=reader_template.root_files_keys,
        extensions_map=reader_template.extensions_map,
        zfill=reader_template.idx_length,
        copy_files=True,
        use_symlinks=False
    )
    out = op(reader)
    writer(out)
    rich.print(f"Removed [red]{len(out) - len(reader)}[/red] samples!")


if __name__ == '__main__':
    filter_by_script()
