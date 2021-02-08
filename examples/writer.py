from pipelime.factories import Bean, BeanFactory
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.writers.filesystem import UnderfolderWriter


folder = '/Users/daniele/Downloads/lego_dataset/lego_00'
d = UnderfolderReader(folder=folder, copy_root_files=True)
print(len(d))

opts = {
    Bean.TYPE_FIELD: UnderfolderReader.bean_name(),
    Bean.ARGS_FIELD: {
        'folder': folder,
        'copy_root_files': True
    }
}

d = BeanFactory.create(opts)
print(len(d))
# writer = UnderfolderWriter(
#     folder='/tmp/gino',
#     root_files_keys=['camera', 'charuco', 'keypoints'],
#     extensions_map={
#     }
# )
# print(len(d))


# writer(d)

# for sample in d:
#     print(list(sample.keys()))
#     break
