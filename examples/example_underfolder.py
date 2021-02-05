from pipelime.sequences.stages import StageIdentity, StageRemap
from pipelime.sequences.readers.filesystem import UnderfolderReader
import cv2
from pipelime.sequences.operations import OperationFilterByQuery, OperationGroupBy, OperationIdentity

folder = '/home/daniele/Desktop/experiments/2021-01-28.PlaygroundDatasets/flow_00'
dataset = UnderfolderReader(folder=folder)
print(len(dataset))

op = OperationGroupBy(field='`metadata.counter`')
op = OperationIdentity()  # OperationFilterByQuery(query='`metadata.')
dataset = op(dataset)


stage = StageRemap({
    'image': 'a'
}, remove_missing=False)
s2 = StageIdentity()

for sample in dataset:

    s = s2(stage(sample))
    # image = sample['image']
    print(list(sample.keys()))
    # cv2.imshow("Image", image)
    # cv2.waitKey(1)
