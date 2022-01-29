import click
from pipelime.pipes.piper import Piper
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
@click.option("-o", "--outuput_folder", type=click.Path(False), required=True)
@click.option("-t", "--times", type=int, default=100)
@click.option("-s", "--sleep", type=float, default=1.0)
@Piper.add_piper_options(inputs=["input_folders"], outputs=["outuput_folder"])
def two(
    input_folders: str,
    outuput_folder: str,
    times: int,
    sleep: float,
    # **piper,
):

    # piper = Piper(**piper)

    print(click.get_current_context().meta)
    print(input_folders, type(input_folders))
    print(outuput_folder, type(outuput_folder))

    for i in range(times):
        print(i)
        # piper.log_value("progress", i)
        time.sleep(sleep)


if __name__ == "__main__":
    two()
