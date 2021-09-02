from pipelime.sequences.writers.h5 import H5Writer
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.readers.h5 import H5Reader
import tempfile
import rich
from pathlib import Path
import time


# ➡️➡️➡️ Reader
readers = {
    'underfolder': UnderfolderReader(folder='/tmp/whilly', copy_root_files=True, lazy_samples=True),
    'h5': H5Reader(filename='/tmp/dataset.h5', copy_root_files=True, lazy_samples=True)
}

for name, reader in readers.items():
    t1 = time.perf_counter()
    mixup = []
    for sample in reader:
        for key in sample.keys():
            mixup.append((key, type(sample[key])))

    t2 = time.perf_counter()
    print(f"{name} [time]", t2 - t1)
