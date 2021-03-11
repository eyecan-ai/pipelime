from pipelime.sequences.validation import SchemaLoader
from pipelime.sequences.samples import Sample
from pipelime.sequences.readers.filesystem import UnderfolderReader


s = SchemaLoader().load(filename='_schema.schema')

folder = '/home/daniele/Desktop/experiments/2021-01-28.PlaygroundDatasets/lego_00'
reader = UnderfolderReader(folder=folder)

for sample in reader:
    sample: Sample
    print(sample.id)

    sample.validate(s, deep=False)
