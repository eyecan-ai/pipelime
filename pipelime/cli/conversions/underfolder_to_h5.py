import click


@click.command("underfolder_to_h5", help="Converts Underfolder to H5")
@click.option("-i", "--input_folder", required=True, type=str, help="Input Underfolder")
@click.option("-o", "--output_filename", required=True, type=str, help="Output H5 File")
@click.option(
    "-c", "--copy_root_files", default=True, type=bool, help="Copy root files"
)
def underfolder_to_h5(input_folder, output_filename, copy_root_files):

    import rich

    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.h5 import H5Writer

    reader = UnderfolderReader(folder=input_folder, copy_root_files=copy_root_files)
    writer = H5Writer(filename=output_filename)
    writer(reader)
    rich.print("H5 Writer output to:", output_filename)
