import click
from pipelime.pipes.piper import Piper, PiperCommand
import time


@click.command("gino")
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
    "--outuput_folder",
    type=click.Path(False),
    required=True,
)
@click.option(
    "-t",
    "--times",
    type=int,
    default=100,
)
@click.option(
    "-s",
    "--sleep",
    type=float,
    default=1.0,
)
@Piper.piper_command_options(
    inputs=["input_folders"],
    outputs=["outuput_folder"],
)
def two(
    input_folders: str,
    outuput_folder: str,
    times: int,
    sleep: float,
    **piper_kwargs,
):

    PiperCommand()

    for i in range(times):
        PiperCommand().log("XXX", {"a": "b"})
        time.sleep(sleep)


if __name__ == "__main__":
    two()
