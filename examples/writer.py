from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.writers.filesystem import UnderfolderWriter


folder = '/Users/daniele/Downloads/lego_dataset/lego_00'
d = UnderfolderReader(folder=folder, copy_root_files=True)

writer = UnderfolderWriter(
    folder='/tmp/gino',
    root_files_keys=['camera', 'charuco', 'keypoints'],
    extensions_map={
    }
)
print(len(d))


writer(d)

# for sample in d:
#     print(list(sample.keys()))
#     break
