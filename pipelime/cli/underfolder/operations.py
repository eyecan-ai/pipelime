

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
        prototype_reader = datasets[0]
        template = prototype_reader.get_filesystem_template()

        # operations
        op_sum = OperationSum()
        op_reindex = OperationResetIndices()
        output_dataset = op_reindex(op_sum(datasets))

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
    op_sum = OperationSubsample(factor=int(factor) if factor > 1 else factor)
    op_reindex = OperationResetIndices()
    output_dataset = op_reindex(op_sum(dataset))

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
    op_sum = OperationSubsample(factor=int(factor) if factor > 1 else factor)
    op_reindex = OperationResetIndices()
    output_dataset = op_reindex(op_sum(dataset))

    writer = UnderfolderWriter(
        folder=output_folder,
        copy_files=True,
        root_files_keys=template.root_files_keys,
        extensions_map=template.extensions_map,
        zfill=template.idx_length
    )
    writer(output_dataset)


if __name__ == '__main__':
    operation_sum()
