

import click


@click.command('operation_sum', help='Sum input underfolders')
@click.option('-i', '--input_folders', required=True, multiple=True, type=str, help='Input Underfolder')
@click.option('-o', '--output_folder', required=True, type=str, help='Output Underfolder')
def operation_sum(input_folders, output_folder):

    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter
    from pipelime.sequences.operations import OperationSum, OperationResetIndices

    if len(input_folders) > 0:
        datasets = [UnderfolderReader(folder=x, lazy_samples=True) for x in input_folders]

        # operations
        op_sum = OperationSum()
        op_reindex = OperationResetIndices()
        output_dataset = op_reindex(op_sum(datasets))

        prototype_reader = datasets[0]
        template = prototype_reader.get_filesystem_template()

        writer = UnderfolderWriter(
            folder=output_folder,
            copy_files=True,
            root_files_keys=template.root_files_keys,
            extensions_map=template.extensions_map,
            zfill=template.idx_length
        )
        writer(output_dataset)


@click.command('operation_sum', help='Subsample input underfolder')
@click.option('-i', '--input_folder', required=True, type=str, help='Input Underfolder')
@click.option('-f', '--factor', required=True, type=float, help='Subsamplig factor. INT (1,inf) or FLOAT [0.0,1.0]')
@click.option('-o', '--output_folder', required=True, type=str, help='Output Underfolder')
def operation_subsample(input_folder, factor, output_folder):

    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter
    from pipelime.sequences.operations import OperationSubsample, OperationResetIndices

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_filesystem_template()

    # operations
    op_subsample = OperationSubsample(factor=int(factor) if factor > 1 else factor)
    op_reindex = OperationResetIndices()
    output_dataset = op_reindex(op_subsample(dataset))

    writer = UnderfolderWriter(
        folder=output_folder,
        copy_files=True,
        root_files_keys=template.root_files_keys,
        extensions_map=template.extensions_map
    )
    writer(output_dataset)


@click.command('operation_shuffle', help='Shuffle input underfolder')
@click.option('-i', '--input_folder', required=True, type=str, help='Input Underfolder')
@click.option('-o', '--output_folder', required=True, type=str, help='Output Underfolder')
@click.option('-s', '--seed', default=-1, type=int, help='Random seed')
def operation_shuffle(input_folder, output_folder, seed):

    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter
    from pipelime.sequences.operations import OperationShuffle, OperationResetIndices

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_filesystem_template()

    # operations
    op_shuffle = OperationShuffle(seed=seed)
    op_reindex = OperationResetIndices()
    output_dataset = op_reindex(op_shuffle(dataset))

    writer = UnderfolderWriter(
        folder=output_folder,
        copy_files=True,
        root_files_keys=template.root_files_keys,
        extensions_map=template.extensions_map
    )
    writer(output_dataset)


@click.command('operation_split', help='Split input underfolder')
@click.option('-i', '--input_folder', required=True, type=str, help='Input Underfolder')
@click.option('-s', '--splits', required=True, multiple=True, nargs=2, help='Splits map')
def operation_split(input_folder, splits):

    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter
    from pipelime.sequences.operations import OperationSplits, OperationResetIndices
    from functools import reduce

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_filesystem_template()

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
            extensions_map=template.extensions_map
        )
        writer(op_reindex(dataset))


@click.command('operation_filterbyquery', help='Filter input underfolder by query')
@click.option('-i', '--input_folder', required=True, type=str, help='Input Underfolder')
@click.option('-q', '--query', required=True, type=str, help='Filtering query')
@click.option('-o', '--output_folder', required=True, type=str, help='Output Underfolder for positive matches')
def operation_filterbyquery(input_folder, query, output_folder):

    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter
    from pipelime.sequences.operations import OperationFilterByQuery, OperationResetIndices

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_filesystem_template()

    # operations
    op_filterbyquery = OperationFilterByQuery(query=query)
    op_reindex = OperationResetIndices()
    output_dataset = op_reindex(op_filterbyquery(dataset))

    writer = UnderfolderWriter(
        folder=output_folder,
        copy_files=True,
        root_files_keys=template.root_files_keys,
        extensions_map=template.extensions_map
    )
    writer(output_dataset)


@click.command('operation_filterbyquery', help='Filter input underfolder by query')
@click.option('-i', '--input_folder', required=True, type=str, help='Input Underfolder')
@click.option('-q', '--query', required=True, type=str, help='Filtering query')
@click.option('-o1', '--output_folder_1', required=True, type=str, help='Output Underfolder for positive matches')
@click.option('-o2', '--output_folder_2', required=True, type=str, help='Output Underfolder for negative matches')
def operation_splitbyquery(input_folder, query, output_folder_1, output_folder_2):

    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter
    from pipelime.sequences.operations import OperationSplitByQuery, OperationResetIndices

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_filesystem_template()

    # operations
    op_splitbyquery = OperationSplitByQuery(query=query)
    op_reindex = OperationResetIndices()
    output_datasets = op_splitbyquery(dataset)

    output_folders = [
        output_folder_1,
        output_folder_2
    ]

    for output_folder, output_dataset in zip(output_folders, output_datasets):
        writer = UnderfolderWriter(
            folder=output_folder,
            copy_files=True,
            root_files_keys=template.root_files_keys,
            extensions_map=template.extensions_map
        )
        writer(op_reindex(output_dataset))


if __name__ == '__main__':
    operation_filterbyquery()
