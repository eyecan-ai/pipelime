from pipelime.augmentations.factory import AugmentationsFactory
from choixe.configurations import XConfig
from albumentations.core.serialization import SerializableMeta
from albumentations.core.six import add_metaclass
import rich
import albumentations as A
import cv2
import pipelime as PL

# A.core.serialization.SerializableMeta.__new__(PadIfNeededV2, 'PadIfNeededV2', )


transform = A.Compose(
    [
        A.RandomCrop(width=450, height=450),
        A.HorizontalFlip(p=0.5),
        A.RandomBrightnessContrast(p=0.2),
    ],
    bbox_params=A.BboxParams(format="coco"),
    keypoint_params=A.KeypointParams(format="xy", label_fields=["category_id"]),
)


#
# transform = A.RGBShift()


# d = A.to_dict(transform)


# cfg = XConfig.from_dict(d)
# rich.print(cfg.to_dict(discard_private_qualifiers=False))
# cfg.save_to('/tmp/out.yml')
