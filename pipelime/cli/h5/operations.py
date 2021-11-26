from pathlib import Path

import click


@click.command("summary", help="Prints the summary of a h5 dataset")
@click.option("-i", "--input", "path", type=Path, required=True, help="Input dataset")
@click.option(
    "-o",
    "--order_by",
    type=click.Choice(["name", "root_item", "count", "encoding", "typeinfo"]),
    default="name",
    help="Sort by column value",
)
@click.option("-R", "--reversed", "reversed_", is_flag=True, help="Reverse sorting")
@click.option(
    "-k",
    "--max_samples",
    default=3,
    type=int,
    help="Maximum number of samples to read from the dataset",
)
def summary(path, order_by, reversed_, max_samples):

    from pipelime.cli.summary import print_summary
    from pipelime.sequences.readers.h5 import H5Reader

    reader = H5Reader(path)
    print_summary(
        reader, order_by=order_by, reversed_=reversed_, max_samples=max_samples
    )
