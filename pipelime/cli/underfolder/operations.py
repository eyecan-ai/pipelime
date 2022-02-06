from pathlib import Path
from typing import Sequence
import click
from pipelime.pipes.piper import Piper, PiperCommand


@click.command("sum", help="Sum input underfolders")
@click.option(
    "-i",
    "--input_folders",
    required=True,
    multiple=True,
    type=click.Path(exists=True),
    help="Input Underfolder [multiple]",
)
@click.option(
    "-o",
    "--output_folder",
    required=True,
    type=click.Path(),
    help="Output Underfolder",
)
@click.option(
    "-c",
    "--convert_root_file",
    multiple=True,
    type=str,
    help="Convert a root file into an item to avoid conflicts [multiple]",
)
@Piper.command(inputs=["input_folders"], outputs=["output_folder"])
def operation_sum(
    input_folders: Sequence[str],
    output_folder: str,
    convert_root_file: Sequence[str],
):

    from pipelime.sequences.operations import OperationResetIndices, OperationSum
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    pipercmd = PiperCommand.instance

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
            progress_callback=pipercmd.generate_progress_callback(),
        )
        writer(output_dataset)


@click.command("mix", help="Mix input underfolders")
@click.option(
    "-i",
    "--input_folders",
    required=True,
    multiple=True,
    type=click.Path(exists=True),
    help="Input Underfolder [multiple]",
)
@click.option(
    "-o",
    "--output_folder",
    required=True,
    type=click.Path(),
    help="Output Underfolder",
)
@Piper.command(inputs=["input_folders"], outputs=["output_folder"])
def operation_mix(
    input_folders: Sequence[str],
    output_folder: str,
):

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
                "Input underfolders must have the same length and their item sets must "
                f"be disjoint: {e}"
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
            progress_callback=PiperCommand.instance.generate_progress_callback(),
        )
        writer(output_dataset)


@click.command("subsample", help="Subsample input underfolder")
@click.option(
    "-i",
    "--input_folder",
    required=True,
    type=click.Path(exists=True),
    help="Input Underfolder",
)
@click.option(
    "-f",
    "--factor",
    required=True,
    type=float,
    help="Subsamplig factor. INT (1,inf) or FLOAT [0.0,1.0]",
)
@click.option(
    "-s",
    "--start",
    type=float,
    default=0,
    help="Subsamplig start. INT (0,inf) or FLOAT [0.0,1.0]",
)
@click.option(
    "-o",
    "--output_folder",
    required=True,
    type=click.Path(),
    help="Output Underfolder",
)
@Piper.command(inputs=["input_folder"], outputs=["output_folder"])
def operation_subsample(
    input_folder: str,
    factor: float,
    start: float,
    output_folder: str,
):

    from pipelime.sequences.operations import OperationResetIndices, OperationSubsample
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    PiperCommand.instance

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_reader_template()

    # operations
    op_subsample = OperationSubsample(
        factor=int(factor) if factor > 1 else factor,
        start=int(start) if factor > 1 else start,
    )
    op_reindex = OperationResetIndices()
    output_dataset = op_reindex(op_subsample(dataset))

    writer = UnderfolderWriter(
        folder=output_folder,
        copy_files=True,
        root_files_keys=template.root_files_keys,
        extensions_map=template.extensions_map,
        progress_callback=PiperCommand.instance.generate_progress_callback(),
    )
    writer(output_dataset)


@click.command("shuffle", help="Shuffle input underfolder")
@click.option(
    "-i",
    "--input_folder",
    required=True,
    type=click.Path(exists=True),
    help="Input Underfolder",
)
@click.option(
    "-o",
    "--output_folder",
    required=True,
    type=click.Path(),
    help="Output Underfolder",
)
@click.option(
    "-s",
    "--seed",
    default=-1,
    type=int,
    help="Random seed",
)
@Piper.command(inputs=["input_folder"], outputs=["output_folder"])
def operation_shuffle(
    input_folder: str,
    output_folder: str,
    seed: int,
):

    PiperCommand.instance

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
        progress_callback=PiperCommand.instance.generate_progress_callback(),
    )
    writer(output_dataset)


@click.command("split", help="Split input underfolder")
@click.option(
    "-i",
    "--input_folder",
    required=True,
    type=click.Path(exists=True),
    help="Input Underfolder",
)
@click.option(
    "-o",
    "--output_folders",
    required=True,
    multiple=True,
    type=click.Path(),
    help="Splits path [multple]",
)
@click.option(
    "-s",
    "--splits",
    required=True,
    multiple=True,
    type=float,
    help="Splits percentages [multple]",
)
@Piper.command(inputs=["input_folder"], outputs=["output_folders"])
def operation_split(
    input_folder: str,
    output_folders: Sequence[str],
    splits: Sequence[float],
):

    from functools import reduce
    from pipelime.sequences.operations import OperationResetIndices, OperationSplits
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    PiperCommand.instance

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_reader_template()

    # splitmap for nargs
    if len(output_folders) != len(splits):
        raise click.UsageError(
            "Number of splits must be equal to number of output folders"
        )

    split_map = {str(k): float(v) for k, v in zip(output_folders, splits)}
    cumulative = reduce(lambda a, b: a + b, split_map.values())
    if cumulative != 1.0:
        raise click.UsageError("Sums of splits percentages must be = 1.0")

    # operations
    op_splits = OperationSplits(split_map=split_map)
    op_reindex = OperationResetIndices()
    output_datasets_map = op_splits(dataset)

    for index, (path, dataset) in enumerate(output_datasets_map.items()):
        writer = UnderfolderWriter(
            folder=path,
            copy_files=True,
            root_files_keys=template.root_files_keys,
            extensions_map=template.extensions_map,
            progress_callback=PiperCommand.instance.generate_progress_callback(
                chunk_index=index,
                total_chunks=len(output_datasets_map),
            ),
        )
        writer(op_reindex(dataset))


@click.command("filter_by_query", help="Filter input underfolder by query")
@click.option(
    "-i",
    "--input_folder",
    required=True,
    type=click.Path(exists=True),
    help="Input Underfolder",
)
@click.option(
    "-q",
    "--query",
    required=True,
    type=str,
    help="Filtering query",
)
@click.option(
    "-o",
    "--output_folder",
    required=True,
    type=click.Path(),
    help="Output Underfolder for positive matches",
)
@Piper.command(inputs=["input_folder"], outputs=["output_folder"])
def operation_filterbyquery(
    input_folder: str,
    query: str,
    output_folder: str,
):

    from pipelime.sequences.operations import (
        OperationFilterByQuery,
        OperationResetIndices,
    )
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    PiperCommand.instance

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
        progress_callback=PiperCommand.instance.generate_progress_callback(),
    )
    writer(output_dataset)


@click.command("split_by_query", help="Filter input underfolder by query")
@click.option(
    "-i",
    "--input_folder",
    required=True,
    type=click.Path(exists=True),
    help="Input Underfolder",
)
@click.option(
    "-q",
    "--query",
    required=True,
    type=str,
    help="Filtering query",
)
@click.option(
    "-o1",
    "--output_folder_1",
    required=True,
    type=click.Path(),
    help="Output Underfolder for positive matches",
)
@click.option(
    "-o2",
    "--output_folder_2",
    required=True,
    type=click.Path(),
    help="Output Underfolder for negative matches",
)
@Piper.command(
    inputs=["input_folder"],
    outputs=["output_folder_1", "output_folder_2"],
)
def operation_splitbyquery(
    input_folder: str,
    query: str,
    output_folder_1: str,
    output_folder_2: str,
):

    from pipelime.sequences.operations import (
        OperationResetIndices,
        OperationSplitByQuery,
    )
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    PiperCommand.instance

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_reader_template()

    # operations
    op_splitbyquery = OperationSplitByQuery(query=query)
    op_reindex = OperationResetIndices()
    output_datasets = op_splitbyquery(dataset)

    output_folders = [output_folder_1, output_folder_2]

    for index, (output_folder, output_dataset) in enumerate(
        zip(output_folders, output_datasets)
    ):
        writer = UnderfolderWriter(
            folder=output_folder,
            copy_files=True,
            root_files_keys=template.root_files_keys,
            extensions_map=template.extensions_map,
            progress_callback=PiperCommand.instance.generate_progress_callback(
                chunk_index=index, total_chunks=len(output_folders)
            ),
        )
        writer(op_reindex(output_dataset))


@click.command("filter_by_script", help="Filter input underfolder by query")
@click.option(
    "-i",
    "--input_folder",
    required=True,
    type=click.Path(exists=True),
    help="Input Underfolder",
)
@click.option(
    "-s",
    "--script",
    required=True,
    type=str,
    help="Filtering python script",
)
@click.option(
    "-o",
    "--output_folder",
    required=True,
    type=click.Path(),
    help="Output Underfolder for positive matches",
)
@Piper.command(inputs=["input_folder"], outputs=["output_folder"])
def operation_filterbyscript(
    input_folder: str,
    script: str,
    output_folder: str,
):

    from pipelime.sequences.operations import (
        OperationFilterByScript,
        OperationResetIndices,
    )
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    PiperCommand.instance

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
        progress_callback=PiperCommand.instance.generate_progress_callback(),
    )
    writer(output_dataset)


@click.command("filter_keys", help="Filter input underfolder keys")
@click.option(
    "-i",
    "--input_folder",
    required=True,
    type=click.Path(exists=True),
    help="Input Underfolder",
)
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
    type=click.Path(),
    help="Output Underfolder for positive matches",
)
@Piper.command(inputs=["input_folder"], outputs=["output_folder"])
def operation_filterkeys(
    input_folder: str,
    keys: Sequence[str],
    negate: bool,
    output_folder: str,
):

    from pipelime.sequences.operations import OperationFilterKeys, OperationResetIndices
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    PiperCommand.instance

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
        progress_callback=PiperCommand.instance.generate_progress_callback(),
    )
    writer(output_dataset)


@click.command("order_by", help="Order input underfolder samples")
@click.option(
    "-i",
    "--input_folder",
    required=True,
    type=click.Path(exists=True),
    help="Input Underfolder",
)
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
    type=click.Path(),
    help="Output Underfolder for positive matches",
)
@Piper.command(inputs=["input_folder"], outputs=["output_folder"])
def operation_orderby(
    input_folder: str,
    keys: Sequence[str],
    output_folder: str,
):

    from pipelime.sequences.operations import OperationOrderBy, OperationResetIndices
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    PiperCommand.instance

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
        progress_callback=PiperCommand.instance.generate_progress_callback(),
    )
    writer(output_dataset)


@click.command("split_by_value", help="Split input underfolder by value")
@click.option(
    "-i",
    "--input_folder",
    required=True,
    type=click.Path(exists=True),
    help="Input Underfolder",
)
@click.option(
    "-k",
    "--key",
    required=True,
    type=str,
    help="split key",
)
@click.option(
    "-o",
    "--output_folder",
    required=True,
    type=click.Path(),
    help="Output parent folder",
)
@click.option(
    "-p",
    "--output_prefix",
    default="split",
    type=str,
    help="Prefix for each output underfolder",
)
def operation_split_by_value(
    input_folder: str,
    key: str,
    output_folder: str,
    output_prefix: str,
):

    # #### CANNOT BE PIPER-ED!! DYNAMIC OUTPUTS PATHS SHOULD BE AVOIDED #####

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
            Path(output_folder) / f"{output_prefix}_{str(i).zfill(zfill)}",
            root_files_keys=template.root_files_keys,
            extensions_map=template.extensions_map,
        )(split)


@click.command("group_by", help="Group input underfolder by key")
@click.option(
    "-i",
    "--input_folder",
    required=True,
    type=click.Path(exists=True),
    help="Input Underfolder",
)
@click.option(
    "-k",
    "--key",
    required=True,
    type=str,
    help="Grouping keys",
)
@click.option(
    "-o",
    "--output_folder",
    required=True,
    type=click.Path(),
    help="Output Underfolder for grouped dataset",
)
@Piper.command(inputs=["input_folder"], outputs=["output_folder"])
def operation_groupby(
    input_folder: str,
    key: str,
    output_folder: str,
):

    from pipelime.sequences.operations import OperationGroupBy, OperationResetIndices
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    PiperCommand.instance

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)

    # operations
    op_filterbyquery = OperationGroupBy(field=key)
    op_reindex = OperationResetIndices()
    output_dataset = op_reindex(op_filterbyquery(dataset))

    writer = UnderfolderWriter(
        folder=output_folder,
        copy_files=True,
        progress_callback=PiperCommand.instance.generate_progress_callback(),
    )
    writer(output_dataset)


@click.command("summary", help="Prints the summary of an underfolder dataset")
@click.option(
    "-i",
    "--input",
    "path",
    type=click.Path(exists=True),
    required=True,
    help="Input dataset",
)
@click.option(
    "-o",
    "--order_by",
    type=click.Choice(["name", "root_item", "count", "encoding", "typeinfo"]),
    default="name",
    help="Sort by column value",
)
@click.option(
    "-R",
    "--reversed",
    "reversed_",
    is_flag=True,
    help="Reverse sorting",
)
@click.option(
    "-k",
    "--max_samples",
    default=3,
    type=int,
    help="Maximum number of samples to read from the dataset",
)
def summary(
    path: str,
    order_by: str,
    reversed_: str,
    max_samples: str,
):

    from pipelime.cli.summary import print_summary
    from pipelime.sequences.readers.filesystem import UnderfolderReader

    reader = UnderfolderReader(path)
    print_summary(
        reader, order_by=order_by, reversed_=reversed_, max_samples=max_samples
    )


@click.command("remap_keys", help="Remap sample keys")
@click.option(
    "-i",
    "--input_folder",
    required=True,
    type=click.Path(exists=True),
    help="Input Underfolder",
)
@click.option(
    "-k",
    "--keys",
    required=True,
    type=str,
    multiple=True,
    nargs=2,
    help="Mapping (source key, remapped key)",
)
@click.option(
    "-R",
    "--remove",
    "remove_",
    is_flag=True,
    help="TRUE to remove not mapped keys",
)
@click.option(
    "-o",
    "--output_folder",
    required=True,
    type=click.Path(),
    help="Output Underfolder for positive matches",
)
@Piper.command(inputs=["input_folder"], outputs=["output_folder"])
def operation_remap_keys(
    input_folder: str,
    keys: Sequence[str],
    remove_: bool,
    output_folder: str,
):

    from pipelime.sequences.operations import (
        OperationRemapKeys,
    )
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    PiperCommand.instance

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_reader_template()

    # operations
    keys_map = {k0: k1 for k0, k1 in keys}

    # update root files and extensions map
    for k0, k1 in keys_map.items():
        if k0 in template.root_files_keys:
            template.root_files_keys.append(k1)
        if k0 in template.extensions_map:
            template.extensions_map[k1] = template.extensions_map[k0]

    op = OperationRemapKeys(remap=keys_map, remove_missing=remove_)
    output_dataset = op(dataset)

    writer = UnderfolderWriter(
        folder=output_folder,
        copy_files=True,
        root_files_keys=template.root_files_keys,
        extensions_map=template.extensions_map,
        progress_callback=PiperCommand.instance.generate_progress_callback(),
    )
    writer(output_dataset)


@click.command("upload", help="Upload underfolder data to remote")
@click.option("-i", "--input_folder", required=True, type=str, help="Input Underfolder")
@click.option(
    "-r",
    "--remote",
    required=True,
    multiple=True,
    help="For each remote you must provide "
    "'<scheme>://<netloc>/<base_path>"
    "[?<init-kw>=<init-value>:<init-kw>=<init-value>...]'"
    " (repeat for each remote)",
)
@click.option(
    "-k",
    "--key",
    required=True,
    multiple=True,
    help="Keys to upload (repeat for each key)",
)
@click.option(
    "--copy",
    "copy_mode",
    flag_value="copy",
    help="Always deep copy source data (default)",
    default=True,
)
@click.option(
    "--symlink", "copy_mode", flag_value="symlink", help="Sym-link source data"
)
@click.option(
    "--hardlink", "copy_mode", flag_value="hardlink", help="Hard-link source data"
)
@click.option(
    "-o", "--output_folder", required=True, type=str, help="Output Underfolder"
)
def upload_to_remote(input_folder, remote, key, copy_mode, output_folder):
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriterV2
    from pipelime.sequences.samples import SamplesSequence
    from pipelime.sequences.stages import StageUploadToRemote
    from urllib.parse import urlparse, unquote_plus

    def _convert_val(val: str):
        if val == "True":
            return True
        if val == "False":
            return False
        try:
            num = int(val)
            return num
        except ValueError:
            pass
        try:
            num = float(val)
            return num
        except ValueError:
            pass
        return val

    remote_params = [urlparse(rm) for rm in remote]
    remote_params = [
        StageUploadToRemote.RemoteParams(
            scheme=rm.scheme,
            netloc=rm.netloc,
            base_path=unquote_plus(rm.path)[1:],
            init_args={
                kw.split("=", 1)[0]: _convert_val(kw.split("=", 1)[1])
                for kw in rm.query.split(":")
                if len(kw) >= 3 and "=" in kw
            },
        )
        for rm in remote_params
    ]

    dataset = UnderfolderReader(folder=input_folder, lazy_samples=True)
    template = dataset.get_reader_template()

    sseq = SamplesSequence(
        dataset,
        stage=StageUploadToRemote(
            remotes=remote_params,
            key_ext_map={k: template.extensions_map.get(k) for k in key},
        ),
    )

    # save uploaded keys with .remote extension
    for k in key:
        template.extensions_map[k] = "remote"

    writer = UnderfolderWriterV2(
        folder=output_folder,
        copy_mode=UnderfolderWriterV2.CopyMode.HARD_LINK
        if copy_mode == "hardlink"
        else (
            UnderfolderWriterV2.CopyMode.SYM_LINK
            if copy_mode == "symlink"
            else UnderfolderWriterV2.CopyMode.DEEP_COPY
        ),
        reader_template=template,
    )
    writer(sseq)
