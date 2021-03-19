import re
from pipelime.factories import Bean, BeanFactory
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.writers.filesystem import UnderfolderWriter
from pipelime.sequences.operations import OperationFilterByQuery, OperationSubsample

folder = '/Users/daniele/Downloads/lego_dataset/lego_00'
d = UnderfolderReader(folder=folder, copy_root_files=True)
print(len(d))


op = OperationFilterByQuery(query='`metadata.tag` == "black"')
op2 = OperationSubsample(factor=0.1)
d = op2(op(d))
print(len(d))

extensions_map = {
    '.*image.*': 'jpg',
    '.*mask.*': 'png',
    '.*pose.*|.*keypoint.*': 'txt',
    '.*metadata.*|.*camera.*|.*charuco.*': 'yml',
}

w = UnderfolderWriter(
    folder='/tmp/gino',
    root_files_keys=['.*camera.*|.*charuco.*|.*keypoints.*'],
    extensions_map=None
)
w(d)

# print(re.match('.*image.*', '_image0'))
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
