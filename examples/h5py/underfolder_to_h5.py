from pipelime.sequences.writers.h5 import H5Writer
from pipelime.sequences.readers.filesystem import UnderfolderReader
import tempfile
import rich
from pathlib import Path

# ➡️➡️➡️ Reader
reader = UnderfolderReader(
    folder="../data/underfolder/example_dataset", copy_root_files=True
)
reader_template = reader.get_reader_template()

# ➡️➡️➡️ Writer with manual template input
writer_filename = Path(tempfile.mkdtemp()) / "dataset.h5"
writer = H5Writer(
    filename=writer_filename,
    root_files_keys=reader_template.root_files_keys,
)
writer(reader)
rich.print("H5 Writer output to:", writer_filename)
