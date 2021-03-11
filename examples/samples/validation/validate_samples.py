from pipelime.sequences.validation import SchemaLoader
from pipelime.sequences.samples import PlainSample, SamplesSequence
import numpy as np
import rich

# Creates samples sequence
N = 25
samples = []
for i in range(N):
    samples.append(
        PlainSample({
            'counter': i,
            'name': f'sample_{str(i).zfill(10)}',
            'data': np.random.uniform(-1, 1, (100, 100, 3))
        })
    )
sequence = SamplesSequence(samples=samples)

# validate sample (should be used to check keys only)
# deep=False will load a keys only dict
rich.print("Simple validation:")
schema = SchemaLoader.load('simple_schema.schema')
for idx, sample in enumerate(sequence):
    sample.validate(schema, deep=False)
    rich.print("\tValidating", idx, 'OK')

# validate sample in depth
# deep=True will load sample content in lazy samples
rich.print("In-depth validation:")
schema = SchemaLoader.load('deep_schema.schema')
for idx, sample in enumerate(sequence):
    sample.validate(schema, deep=True)
    rich.print("\tValidating", idx, 'OK')
