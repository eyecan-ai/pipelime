
import numpy as np
import cv2
import click
from pathlib import Path
import yaml
import json
from pipelime.tools.toydataset import ToyDatasetGenerator


@click.command("generate_toy_dataset")
@click.option('--output_folder', type=str, required=True, help="Output folder")
@click.option('--size', type=int, required=True, help="Number of output samples")
@click.option('--image_size', type=int, default=256, help="Output image size")
@click.option('--zfill', type=int, default=5, help="Zero padding digits")
def generate_toy_dataset(output_folder, size, image_size, zfill):

    ToyDatasetGenerator.generate_toy_dataset(
        output_folder=output_folder,
        size=size,
        image_size=image_size,
        zfill=zfill
    )


if __name__ == "__main__":
    generate_toy_dataset()
