import click


@click.command("not_a_piper")
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
def not_a_piper(
    input_folders: str,
    output_folders: str,
):
    pass


if __name__ == "__main__":
    not_a_piper()
