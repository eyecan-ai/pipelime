import click


@click.command("subfolders_to_underfolder", help="Converts Subfolders to Underfolder")
@click.option("-i", "--input_folder", required=True, type=str, help="Input H5 Filename")
@click.option(
    "-o", "--output_folder", required=True, type=str, help="Output Underfolder"
)
@click.option(
    "-e",
    "--extension",
    required=True,
    type=str,
    help="Image extension to include in conversion",
)
def subfolders_to_underfolder(input_folder: str, output_folder: str, extension: str):

    import rich

    from pipelime.converters.subfolders2underfolder import Subfolders2Underfolder

    converter = Subfolders2Underfolder(folder=input_folder, images_extension=extension)
    converter.convert(output_folder)
    rich.print("Underfolder Writer output to:", output_folder)
