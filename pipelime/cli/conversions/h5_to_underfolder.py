import click


@click.command("h5_to_underfolder", help="Converts H5 dataset to Underfolder")
@click.option(
    "-i", "--input_filename", required=True, type=str, help="Input H5 Filename"
)
@click.option(
    "-o", "--output_folder", required=True, type=str, help="Output Underfolder"
)
@click.option(
    "-r",
    "--reference_folder",
    default="",
    type=str,
    help="Reference Underfolder to retrieve extensions and rootfiles",
)
@click.option(
    "-c", "--copy_root_files", default=True, type=bool, help="Copy root files"
)
def h5_to_underfolder(input_filename, output_folder, reference_folder, copy_root_files):

    import rich

    from pipelime.sequences.readers.base import ReaderTemplate
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.readers.h5 import H5Reader
    from pipelime.sequences.writers.filesystem import UnderfolderWriter

    template = ReaderTemplate(
        extensions_map=None,
        root_files_keys=None,
    )
    if len(reference_folder) > 0:
        template = UnderfolderReader.get_reader_template_from_folder(reference_folder)

    reader = H5Reader(
        filename=input_filename, copy_root_files=copy_root_files, lazy_samples=True
    )

    writer = UnderfolderWriter(
        folder=output_folder,
        root_files_keys=template.root_files_keys,
        extensions_map=template.extensions_map,
        zfill=template.idx_length,
    )
    writer(reader)
    rich.print("Underfolder Writer output to:", output_folder)
