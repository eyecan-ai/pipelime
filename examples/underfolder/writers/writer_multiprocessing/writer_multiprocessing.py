from pipelime.sequences.samples import PlainSample, SamplesSequence
from pipelime.sequences.writers.filesystem import UnderfolderWriter
import tempfile
import rich
from rich.progress import track
import numpy as np
import time
import shutil
import multiprocessing


def new_sample(i) -> PlainSample:
    return PlainSample(
        data={"image": np.random.uniform(0, 255, (256, 256, 3)).astype(np.uint8)}
    )


# ➡️➡️➡️ Reader
N = 1000
pool = multiprocessing.Pool()
samples = list(track(pool.imap_unordered(new_sample, range(N)), total=N))
sequence = SamplesSequence(samples=samples)

print(len(sequence))

workers_options = [0, 1, 2, 3, 4, -1]

for num_workers in workers_options:
    # ➡️➡️➡️ Writer with manual template input
    writer_folder = tempfile.mkdtemp()
    writer = UnderfolderWriter(
        folder=writer_folder, extensions_map={"image": "png"}, num_workers=num_workers
    )
    t1 = time.perf_counter()
    writer(sequence)
    t2 = time.perf_counter()
    rich.print(f"num_workers={num_workers} -> Time: {t2-t1} s")
    shutil.rmtree(writer_folder)
