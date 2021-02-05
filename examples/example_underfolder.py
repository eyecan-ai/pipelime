import rich
import albumentations as A
from albumentations.augmentations.functional import scale
from pipelime.sequences.samples import Sample
from pipelime.sequences.stages import StageAugmentations, StageIdentity, StageRemap
from pipelime.sequences.readers.filesystem import UnderfolderReader
import cv2
from pipelime.sequences.operations import OperationFilterByQuery, OperationGroupBy, OperationIdentity
import numpy as np


def draw_keypoints(keypoints: np.ndarray, image: np.ndarray):

    h, w = image.shape[:2]
    for kp in keypoints:
        print("KP", kp)
        pos = np.array(kp[0:2])  # * np.array([w, h])
        a = kp[2]
        s = kp[3] * 0.7
        direction = np.array([np.cos(-a), np.sin(-a)])

        cv2.circle(image, tuple(pos.astype(int)), radius=5, color=(255, 255, 255))

        pos2 = pos + direction * s
        cv2.arrowedLine(image, tuple(pos.astype(int)), tuple(pos2.astype(int)), (255, 255, 255), 2)


# folder = '/home/daniele/Desktop/experiments/2021-01-28.PlaygroundDatasets/flow_00'
folder = '/tmp/ds'
dataset = UnderfolderReader(folder=folder)
print(len(dataset))

op = OperationGroupBy(field='`metadata.counter`')
op = OperationIdentity()  # OperationFilterByQuery(query='`metadata.')
dataset = op(dataset)

transform = A.Compose([
    # A.RandomCrop(width=450, height=450),
    # A.HorizontalFlip(p=0.5),
    A.ShiftScaleRotate(shift_limit=0.2, scale_limit=0.6, rotate_limit=180, border_mode=cv2.BORDER_CONSTANT, p=1.0)
    # A.RandomBrightnessContrast(p=0.2),
],
    keypoint_params=A.KeypointParams(format='xyas', remove_invisible=True, angle_in_degrees=False),
    bbox_params=A.BboxParams(format='yolo')
)

stage = StageAugmentations(
    transform_cfg=A.to_dict(transform),
    targets={'image': 'image', 'keypoints': 'keypoints'}
)


# stage = StageRemap({
#     'image': 'a'
# }, remove_missing=False)
# s2 = StageIdentity()
[cv2.namedWindow(x, cv2.WINDOW_NORMAL) for x in ["image", "mask", "inst"]]

for sample in dataset:

    sample = stage(sample)

    image = sample['image']
    h, w = image.shape[:2]
    keypoints = sample['keypoints']

    mask = sample['mask']
    inst = sample['inst']

    # idx = keypoints[:, 0]

    # # keypoints = keypoints[:, 1:5]
    # keypoints[:, 0] *= w
    # keypoints[:, 1] *= h

    # print("pre", keypoints)
    # transform.add_targets({'inst': 'mask', 'keypoints': 'keypoints'})
    # _t = transform(image=image, keypoints=keypoints, mask=mask, inst=inst)  # , class_labels=idx)
    # image = _t['image']
    # keypoints = _t['keypoints']
    # # idx = _t['class_labels']
    # mask = _t['mask']
    # inst = _t['inst']
    # print("post", keypoints)
    # print(idx)

    draw_keypoints(keypoints, image)

    mask = (mask - mask.min())/(mask.max() - mask.min())
    inst = (inst - inst.min())/(inst.max() - inst.min())
    cv2.imshow("image", image)
    cv2.imshow("mask", mask)
    cv2.imshow("inst", inst)
    if ord('q') == cv2.waitKey(0):
        break
