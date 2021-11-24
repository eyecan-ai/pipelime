from pathlib import Path

import click


@click.command("sum", help="Sum input underfolders")
@click.option(
    "-i",
    "--input_folders",
    required=True,
    multiple=True,
    type=str,
    help="Input Underfolder [multiple]",
)
@click.option(
    "-o", "--output_folder", required=True, type=str, help="Output Underfolder"
)
@click.option(
    "-c",
    "--convert_root_file",
    multiple=True,
    type=str,
    help="Convert a root file into an item to avoid conflicts [multiple]",
)
def operation_sum(input_folders, output_folder, convert_root_file):

    from pipelime.sequences.operations import OperationResetIndices, OperationSum
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    if len(input_folders) > 0:
        datasets = [
            UnderfolderReader(folder=x, lazy_samples=True) for x in input_folders
        ]

        # operations
        op_sum = OperationSum()
        op_reindex = OperationResetIndices()
        output_dataset = op_reindex(op_sum(datasets))

        prototype_reader = datasets[0]
        template = prototype_reader.get_reader_template()

        writer = UnderfolderWriter(
            folder=output_folder,
            copy_files=True,
            root_files_keys=list(
                set(template.root_files_keys) - set(convert_root_file)
            ),
            extensions_map=template.extensions_map,
            zfill=template.idx_length,
        )
        writer(output_dataset)


@click.command("mix", help="Mix input underfolders")
@click.option(
    "-i",
    "--input_folders",
    required=True,
    multiple=True,
    type=str,
    help="Input Underfolder [multiple]",
)
@click.option(
    "-o", "--output_folder", required=True, type=str, help="Output Underfolder"
)
def operation_mix(input_folders, output_folder):

    from click import ClickException

    from pipelime.sequences.operations import OperationMix
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    if len(input_folders) > 0:
        datasets = [
            UnderfolderReader(folder=x, lazy_samples=True) for x in input_folders
        ]

        # operations
        op_mix = OperationMix()
        try:
            output_dataset = op_mix(datasets)
        except AssertionError as e:
            raise ClickException(
                "Input underfolders must have the same length and their item sets must be disjoint."
            )

        root_files = []
        ext_map = {}
        for d in datasets:
            template = d.get_reader_template()
            root_files.extend(template.root_files_keys)
            ext_map.update(template.extensions_map)

        writer = UnderfolderWriter(
            folder=output_folder,
            copy_files=True,
            root_files_keys=root_files,
            extensions_map=ext_map,
        )
        writer(output_dataset)


@click.command("subsample", help="Subsample input underfolder")
@click.option("-i", "--input_folder", required=True, type=str, help="Input Underfolder")
@click.option(
    "-f",
    "--factor",
    required=True,
    type=float,
    help="Subsamplig factor. INT (1,inf) or FLOAT [0.0,1.0]",
)
@click.option(
    "-o", "--output_folder", required=True, type=str, help="Output Underfolder"
)
def operation_subsample(input_folder, factor, output_folder):

    from pipelime.sequences.operations import OperationResetIndices, OperationSubsample
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_reader_template()

    # operations
    op_subsample = OperationSubsample(factor=int(factor) if factor > 1 else factor)
    op_reindex = OperationResetIndices()
    output_dataset = op_reindex(op_subsample(dataset))

    writer = UnderfolderWriter(
        folder=output_folder,
        copy_files=True,
        root_files_keys=template.root_files_keys,
        extensions_map=template.extensions_map,
    )
    writer(output_dataset)


@click.command("shuffle", help="Shuffle input underfolder")
@click.option("-i", "--input_folder", required=True, type=str, help="Input Underfolder")
@click.option(
    "-o", "--output_folder", required=True, type=str, help="Output Underfolder"
)
@click.option("-s", "--seed", default=-1, type=int, help="Random seed")
def operation_shuffle(input_folder, output_folder, seed):

    from pipelime.sequences.operations import OperationResetIndices, OperationShuffle
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_reader_template()

    # operations
    op_shuffle = OperationShuffle(seed=seed)
    op_reindex = OperationResetIndices()
    output_dataset = op_reindex(op_shuffle(dataset))

    writer = UnderfolderWriter(
        folder=output_folder,
        copy_files=True,
        root_files_keys=template.root_files_keys,
        extensions_map=template.extensions_map,
    )
    writer(output_dataset)


@click.command("split", help="Split input underfolder")
@click.option("-i", "--input_folder", required=True, type=str, help="Input Underfolder")
@click.option(
    "-s",
    "--splits",
    required=True,
    multiple=True,
    nargs=2,
    help="Splits map, pairs (path, percentage) [multple]",
)
def operation_split(input_folder, splits):

    from functools import reduce

    from pipelime.sequences.operations import OperationResetIndices, OperationSplits
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_reader_template()

    # splitmap for nargs
    split_map = {str(k): float(v) for k, v in splits}

    cumulative = reduce(lambda a, b: a + b, split_map.values())
    assert cumulative <= 1.0, "Sums of splits percentages must be <= 1.0"

    # operations
    op_splits = OperationSplits(split_map=split_map)
    op_reindex = OperationResetIndices()
    output_datasets_map = op_splits(dataset)

    for path, dataset in output_datasets_map.items():
        writer = UnderfolderWriter(
            folder=path,
            copy_files=True,
            root_files_keys=template.root_files_keys,
            extensions_map=template.extensions_map,
        )
        writer(op_reindex(dataset))


@click.command("filter_by_query", help="Filter input underfolder by query")
@click.option("-i", "--input_folder", required=True, type=str, help="Input Underfolder")
@click.option("-q", "--query", required=True, type=str, help="Filtering query")
@click.option(
    "-o",
    "--output_folder",
    required=True,
    type=str,
    help="Output Underfolder for positive matches",
)
def operation_filterbyquery(input_folder, query, output_folder):

    from pipelime.sequences.operations import (
        OperationFilterByQuery,
        OperationResetIndices,
    )
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_reader_template()

    # operations
    op_filterbyquery = OperationFilterByQuery(query=query)
    op_reindex = OperationResetIndices()
    output_dataset = op_reindex(op_filterbyquery(dataset))

    writer = UnderfolderWriter(
        folder=output_folder,
        copy_files=True,
        root_files_keys=template.root_files_keys,
        extensions_map=template.extensions_map,
    )
    writer(output_dataset)


@click.command("split_by_query", help="Filter input underfolder by query")
@click.option("-i", "--input_folder", required=True, type=str, help="Input Underfolder")
@click.option("-q", "--query", required=True, type=str, help="Filtering query")
@click.option(
    "-o1",
    "--output_folder_1",
    required=True,
    type=str,
    help="Output Underfolder for positive matches",
)
@click.option(
    "-o2",
    "--output_folder_2",
    required=True,
    type=str,
    help="Output Underfolder for negative matches",
)
def operation_splitbyquery(input_folder, query, output_folder_1, output_folder_2):

    from pipelime.sequences.operations import (
        OperationResetIndices,
        OperationSplitByQuery,
    )
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_reader_template()

    # operations
    op_splitbyquery = OperationSplitByQuery(query=query)
    op_reindex = OperationResetIndices()
    output_datasets = op_splitbyquery(dataset)

    output_folders = [output_folder_1, output_folder_2]

    for output_folder, output_dataset in zip(output_folders, output_datasets):
        writer = UnderfolderWriter(
            folder=output_folder,
            copy_files=True,
            root_files_keys=template.root_files_keys,
            extensions_map=template.extensions_map,
        )
        writer(op_reindex(output_dataset))


@click.command("filter_by_script", help="Filter input underfolder by query")
@click.option("-i", "--input_folder", required=True, type=str, help="Input Underfolder")
@click.option("-s", "--script", required=True, type=str, help="Filtering python script")
@click.option(
    "-o",
    "--output_folder",
    required=True,
    type=str,
    help="Output Underfolder for positive matches",
)
def operation_filterbyscript(input_folder, script, output_folder):

    from pipelime.sequences.operations import (
        OperationFilterByScript,
        OperationResetIndices,
    )
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_reader_template()

    # operations
    op_filterbyquery = OperationFilterByScript(path_or_func=script)
    op_reindex = OperationResetIndices()
    output_dataset = op_reindex(op_filterbyquery(dataset))

    writer = UnderfolderWriter(
        folder=output_folder,
        copy_files=True,
        root_files_keys=template.root_files_keys,
        extensions_map=template.extensions_map,
    )
    writer(output_dataset)


@click.command("filter_keys", help="Filter input underfolder keys")
@click.option("-i", "--input_folder", required=True, type=str, help="Input Underfolder")
@click.option(
    "-k",
    "--keys",
    required=True,
    type=str,
    multiple=True,
    help="Filtering keys [multiple]",
)
@click.option(
    "--negate/--no-negate",
    required=False,
    type=bool,
    help="Negate filtering effets (i.e. all but selected keys)",
)
@click.option(
    "-o",
    "--output_folder",
    required=True,
    type=str,
    help="Output Underfolder for positive matches",
)
def operation_filterkeys(input_folder, keys, negate, output_folder):

    from pipelime.sequences.operations import OperationFilterKeys, OperationResetIndices
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_reader_template()

    # operations
    op_filterbyquery = OperationFilterKeys(keys=keys, negate=negate)
    op_reindex = OperationResetIndices()
    output_dataset = op_reindex(op_filterbyquery(dataset))

    writer = UnderfolderWriter(
        folder=output_folder,
        copy_files=True,
        root_files_keys=template.root_files_keys,
        extensions_map=template.extensions_map,
    )
    writer(output_dataset)


@click.command("order_by", help="Order input underfolder samples")
@click.option("-i", "--input_folder", required=True, type=str, help="Input Underfolder")
@click.option(
    "-k",
    "--keys",
    required=True,
    type=str,
    multiple=True,
    help="Filtering keys [multiple]",
)
@click.option(
    "-o",
    "--output_folder",
    required=True,
    type=str,
    help="Output Underfolder for positive matches",
)
def operation_orderby(input_folder, keys, output_folder):

    from pipelime.sequences.operations import OperationOrderBy, OperationResetIndices
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_reader_template()

    # operations
    op_filterbyquery = OperationOrderBy(order_keys=keys)
    op_reindex = OperationResetIndices()
    output_dataset = op_reindex(op_filterbyquery(dataset))

    writer = UnderfolderWriter(
        folder=output_folder,
        copy_files=True,
        root_files_keys=template.root_files_keys,
        extensions_map=template.extensions_map,
    )
    writer(output_dataset)


@click.command("split_by_value", help="Split input underfolder by value")
@click.option(
    "-i", "--input_folder", required=True, type=Path, help="Input Underfolder"
)
@click.option("-k", "--key", required=True, type=str, help="split key")
@click.option(
    "-o", "--output_folder", required=True, type=Path, help="Output parent folder"
)
@click.option(
    "-p",
    "--output_prefix",
    default="split",
    type=str,
    help="Prefix for each output underfolder",
)
def operation_split_by_value(input_folder, key, output_folder, output_prefix):

    from math import ceil, log10

    from pipelime.sequences.operations import (
        OperationResetIndices,
        OperationSplitByValue,
    )
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    dataset = UnderfolderReader(input_folder)
    template = dataset.get_reader_template()

    op_split = OperationSplitByValue(key=key)
    splits = op_split(dataset)

    zfill = max(ceil(log10(len(splits) + 1)), 1)
    for i, split in enumerate(splits):
        op_reindex = OperationResetIndices()
        split = op_reindex(split)
        UnderfolderWriter(
            output_folder / f"{output_prefix}_{str(i).zfill(zfill)}",
            root_files_keys=template.root_files_keys,
            extensions_map=template.extensions_map,
        )(split)


@click.command("group_by", help="Group input underfolder by key")
@click.option("-i", "--input_folder", required=True, type=str, help="Input Underfolder")
@click.option("-k", "--key", required=True, type=str, help="Grouping keys")
@click.option(
    "-o",
    "--output_folder",
    required=True,
    type=str,
    help="Output Underfolder for positive matches",
)
def operation_groupby(input_folder, key, output_folder):

    from pipelime.sequences.operations import OperationGroupBy, OperationResetIndices
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_reader_template()

    # operations
    op_filterbyquery = OperationGroupBy(field=key)
    op_reindex = OperationResetIndices()
    output_dataset = op_reindex(op_filterbyquery(dataset))

    writer = UnderfolderWriter(
        folder=output_folder,
        copy_files=True,
    )
    writer(output_dataset)


@click.command("summary", help="Prints the summary of an underfolder dataset")
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
    from pipelime.sequences.readers.filesystem import UnderfolderReader

    reader = UnderfolderReader(path)
    print_summary(
        reader, order_by=order_by, reversed_=reversed_, max_samples=max_samples
    )


if __name__ == "__main__":
    operation_filterbyquery()
