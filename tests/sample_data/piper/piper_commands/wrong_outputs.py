import click
from pipelime.pipes.piper import Piper


@click.command("wrong_outputs")
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
    outputs=["output_folder"],
)
def wrong_outputs(
    input_folders: str,
    output_folders: str,
):

    pass


if __name__ == "__main__":
    wrong_outputs()
