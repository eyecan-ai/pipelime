
from pipelime.augmentations.factory import AugmentationsFactory
import albumentations as A
import numpy as np
from choixe.configurations import XConfig


class TestAugmentationsFactory(object):

    def _transform_test_image(self, transform):

        img = np.random.uniform(0., 1., (256, 256, 3))
        img = transform(image=img)

    def test_creation_from_file(self, augmentations_test_configurations):

        for cfg in augmentations_test_configurations:

            transform = AugmentationsFactory.build_from_file(cfg['filename'])
            assert transform is not None

            self._transform_test_image(transform)

    def test_creation_from_cfg(self, augmentations_test_configurations):

        for cfg in augmentations_test_configurations:

            xc = XConfig(filename=cfg['filename']).to_dict()
            transform = AugmentationsFactory.build_from_dict(xc)
            assert transform is not None

            self._transform_test_image(transform)
