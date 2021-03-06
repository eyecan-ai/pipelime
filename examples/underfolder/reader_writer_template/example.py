from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.writers.filesystem import UnderfolderWriter
import tempfile
import rich

# ➡️➡️➡️ Reader
reader = UnderfolderReader(folder='../../data/underfolder/example_dataset', copy_root_files=True)
reader_template = reader.get_filesystem_template()

# ➡️➡️➡️ Writer with manual template input
writer_folder = tempfile.mkdtemp()
writer = UnderfolderWriter(
    folder=writer_folder,
    root_files_keys=reader_template.root_files_keys,
    extensions_map=reader_template.extensions_map,
    zfill=reader_template.idx_length
)
writer(reader)
rich.print("Writer output to:", writer_folder)

# ➡️➡️➡️ Writer with auto template
writer_folder = tempfile.mkdtemp()
writer = UnderfolderWriter(folder=writer_folder)
writer(reader)
rich.print("Writer output to:", writer_folder)
