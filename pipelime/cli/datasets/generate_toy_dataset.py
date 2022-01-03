from pathlib import Path
import click
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.tools.toydataset import ToyDatasetGenerator


@click.command("generate_toy_dataset")
@click.option("-o", "--output_folder", type=str, required=True, help="Output folder")
@click.option("-s", "--size", type=int, required=True, help="Number of output samples")
@click.option("--image_size", type=int, default=256, help="Output image size")
@click.option("--zfill", type=int, default=5, help="Zero padding digits")
@click.option(
    "--suffix", type=str, default="", help="Suffix to add to the output files"
)
def generate_toy_dataset(output_folder, size, image_size, zfill, suffix):

    output_folder = Path(output_folder) / UnderfolderReader.DATA_SUBFOLDER
    ToyDatasetGenerator.generate_toy_dataset(
        output_folder=output_folder,
        size=size,
        image_size=image_size,
        zfill=zfill,
        suffix=suffix,
    )


if __name__ == "__main__":
    generate_toy_dataset()
