import click

from pipelime.pipes.piper import Piper, PiperCommand


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
@click.option(
    "-s",
    "--use_symlinks",
    required=False,
    type=bool,
    default=False,
    help="Use symlinks instead of copying files",
)
@click.option(
    "-w",
    "--num_workers",
    required=False,
    type=int,
    default=0,
    help="Number of workers to use",
)
@Piper.command(
    inputs=["input_folder"],
    outputs=["output_folder"],
)
def subfolders_to_underfolder(
    input_folder: str,
    output_folder: str,
    extension: str,
    use_symlinks: bool,
    num_workers: int,
):

    import rich

    from pipelime.converters.subfolders2underfolder import Subfolders2Underfolder

    converter = Subfolders2Underfolder(
        folder=input_folder,
        images_extension=extension,
        use_symlinks=use_symlinks,
        num_workers=num_workers,
        progress_callback=PiperCommand.instance.generate_progress_callback(),
    )
    converter.convert(output_folder)
    rich.print("Underfolder Writer output to:", output_folder)
