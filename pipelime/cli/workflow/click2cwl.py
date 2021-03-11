from pathlib import Path
import click
from pipelime.workflow.tools.click2cwl import Click2Cwl


@click.command("click2cwl", help="Converts a click script to a cwl file")
@click.option("--script", required=True, type=str, help="Script filename")
@click.option("--output_folder", required=True, type=str, help="Output folder to save the cwl file with the same name of input script")
@click.option("--commands", required=True, type=str, help="Commands associated to the cwl, commands are separated by '.' ")
@click.option("-f", "--forward", "forwards", required=False, multiple=True, type=str, help="Script parameters to forward to cwl output")
def click2cwl(
    script,
    output_folder,
    commands,
    forwards
):
    script = Path(script).absolute().resolve()
    commands = commands.split('.')
    cmd = Click2Cwl.load_click(script)
    assert cmd is not None, "the script doesn't contain any click.Command"
    cwl_script = Click2Cwl.convert_click_to_cwl(cmd, commands, forwards)
    output_file = Path(output_folder) / f'{script.stem}.cwl'
    Click2Cwl.save_cwl(cwl_script, output_file)


if __name__ == "__main__":
    click2cwl()
