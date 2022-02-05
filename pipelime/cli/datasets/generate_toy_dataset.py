import click

from pipelime.pipes.piper import Piper, PiperCommand


@click.command("generate_toy_dataset")
@click.option("-o", "--output_folder", type=str, required=True, help="Output folder")
@click.option("-s", "--size", type=int, required=True, help="Number of output samples")
@click.option("--image_size", type=int, default=256, help="Output image size")
@click.option("--zfill", type=int, default=5, help="Zero padding digits")
@click.option(
    "--max_label", type=int, default=5, help="Generate class labels in range [0, <arg>]"
)
@click.option(
    "--nr_objs",
    type=int,
    nargs=2,
    default=(1, 4),
    help="The number of objects in every image will be in this range (endpoints included)",
)
@click.option(
    "--suffix", type=str, default="", help="Suffix to add to the output files"
)
@Piper.command(outputs=["output_folder"])
def generate_toy_dataset(
    output_folder,
    size,
    image_size,
    zfill,
    max_label,
    nr_objs,
    suffix,
):

    from pathlib import Path
    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.tools.toydataset import ToyDatasetGenerator

    output_folder = Path(output_folder) / UnderfolderReader.DATA_SUBFOLDER
    ToyDatasetGenerator.generate_toy_dataset(
        output_folder=output_folder,
        size=size,
        image_size=image_size,
        zfill=zfill,
        suffix=suffix,
        max_label=max_label,
        objects_number_range=(nr_objs[0], nr_objs[1] + 1),
        progress_callback=PiperCommand.instance.generate_progress_callback(),
    )


if __name__ == "__main__":
    generate_toy_dataset()
