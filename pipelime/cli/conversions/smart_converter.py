from typing import Tuple
import click
from pipelime.pipes.piper import Piper, PiperCommand


@click.command("smart_converter", help="Converts Subfolders to Underfolder")
@click.option("-i", "--input_folder", required=True, type=str, help="Input H5 Filename")
@click.option(
    "-o", "--output_folder", required=True, type=str, help="Output Underfolder"
)
@click.option(
    "-e",
    "--extensions_map",
    required=True,
    type=(str, str),
    multiple=True,
    help="Image extensions map pairs {item_name:extension} as multiple tuples",
)
@Piper.command(
    inputs=["input_folder"],
    outputs=["output_folder"],
)
def smart_converter(
    input_folder: str,
    output_folder: str,
    extensions_map: str,
):

    import rich

    from pipelime.converters.smartconverter import SmartConverter

    extensions_map = {a: b for a, b in extensions_map}
    converter = SmartConverter(
        folder=input_folder,
        extensions_map=extensions_map,
    )
    converter.convert(output_folder)
    rich.print("Underfolder Writer output to:", output_folder)
