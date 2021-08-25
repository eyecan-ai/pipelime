import os
from PIL import Image
from pipelime.sequences.writers.filesystem import UnderfolderWriter
from pipelime.sequences.samples import PlainSample


# Extension with their formats
extensions = {
            'image': 'jpg',
            'image_mask': 'png',
            'image_maskinv': 'png',
            'label': 'txt',
            'metadata': 'json',
            'metadatay': 'yml',
            'points': 'txt',
            'numbers': 'txt',
            'pose': 'txt',
            'cfg': 'yml',
            'tracepen': 'txt'
        }

# Example of a writer
writer = UnderfolderWriter(
    folder=os.path.dirname(os.path.abspath(__file__)),
    extensions_map=extensions
)

samples = []
pil_map = Image.new("RGB", (125, 125), 230)

# This is an example on how to save an image with id 000
samples.append(PlainSample(data = {str('image'): pil_map}, id='000'))

writer(samples)  