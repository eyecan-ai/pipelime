from pathlib import Path
import click
from pipelime.workflow.tools.cwl_template import CwlTemplate


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
    cwl_template = CwlTemplate(script=script, alias=commands, forwards=list(forwards))
    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)
    cwl_template.save_to(output_folder / f'{script.stem}.cwl')


if __name__ == "__main__":
    click2cwl()
