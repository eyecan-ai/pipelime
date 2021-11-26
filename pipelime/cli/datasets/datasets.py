import click

from pipelime.cli.datasets.generate_toy_dataset import generate_toy_dataset


@click.group()
def datasets():
    pass


datasets.add_command(generate_toy_dataset)
