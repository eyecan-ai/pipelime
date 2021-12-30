from pipelime.sequences.readers.filesystem import UnderfolderReader
import rich
import tempfile
from pathlib import Path
from pipelime.sequences.samples import PlainSample, SamplesSequence
from pipelime.sequences.writers.filesystem import UnderfolderWriter

# Base temp folder
base_folder = Path(tempfile.mkdtemp())
rich.print("Working directory:", base_folder)

# Versions as Incremental upgrade of base dataset
versions = ["B", "C", "D", "E", "F"]

# Generate first dataset
last_key = "A"
last_folder = base_folder / last_key
UnderfolderWriter(folder=last_folder)(
    SamplesSequence([PlainSample({last_key: i}, id=i) for i in range(10)])
)

# Generate incremental upgrade datasets
for version in versions:
    # read previous sequence
    reader = UnderfolderReader(folder=last_folder)

    # generate a new sequence whose samples are a modified version of the previous
    new_sequence = SamplesSequence([])
    for sample_index, sample in enumerate(reader):
        new_sequence.samples.append(
            PlainSample({version: sample[last_key] * 2}, id=sample_index)
        )

    # Write dataset
    new_folder = base_folder / version
    UnderfolderWriter(folder=new_folder)(new_sequence)
    rich.print("Writing", version, new_folder)

    # Here the link is created
    UnderfolderReader.link_underfolders(str(new_folder), str(last_folder))

    # update pointers
    last_folder = new_folder
    last_key = version

# Print samples of last reader, which is a cumulative dataset of all versions
last_reader = UnderfolderReader(folder=last_folder)
for sample in last_reader:
    rich.print("Sample:")
    for key in sorted(sample.keys()):
        rich.print("\t", key, ":", sample[key])
