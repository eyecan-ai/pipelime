
from pipelime.sequences.readers.filesystem import UnderfolderReader


folder = '/tmp/gino'
r = UnderfolderReader(folder=folder)

print(len(r))

for sample in r:
    for k in sample:
        if k == 'metadata':
            print(sample[k])
