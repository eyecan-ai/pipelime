import click
from pipelime.pipes.piper import Piper


@click.command()
@click.option(
    "-i",
    "--input_folders",
    type=click.Path(exists=True),
    required=True,
    multiple=True,
    help="The input folder",
)
@click.option(
    "-o",
    "--output_folders",
    type=click.Path(),
    required=True,
    multiple=True,
    help="The input folder",
)
@Piper.command(
    inputs=["input_folders"],
    outputs=["output_folders"],
)
def no_command_name(
    input_folders: str,
    output_folders: str,
):
    pass


if __name__ == "__main__":
    no_command_name()
