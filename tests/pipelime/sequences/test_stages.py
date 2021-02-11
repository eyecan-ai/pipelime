from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.samples import PlainSample
from schema import Schema
from pipelime.sequences.stages import (
    SampleStage, StageAugmentations,
    StageCompose, StageIdentity, StageKeysFilter, StageRemap
)
from pipelime.factories import BeanFactory


def _plug_test(stage: SampleStage):
    """ Test what a generic SampleStage should do

    :param stage: input SampleStage
    :type stage: SampleStage
    """

    assert isinstance(stage, SampleStage)

    restage = stage.from_dict(stage.to_dict())
    assert isinstance(restage, SampleStage)
    assert isinstance(restage.bean_schema(), dict)

    factored = BeanFactory.create(stage.serialize())
    assert isinstance(factored, SampleStage)


class TestStageIdentity(object):

    def test_identity(self):

        s = PlainSample(data={'name': 'sample', 'idx': 111})

        stage = StageIdentity()
        _plug_test(stage)

        out = stage(s)

        assert out == s


class TestStageRemap(object):

    def test_remap(self):

        s = PlainSample(data={'name': 'sample', 'idx': 111})

        stage = StageRemap(remap={'name': 'a', 'idx': 'b', 'not_present': 'x'})
        _plug_test(stage)

        out = stage(s)

        assert 'a' in out
        assert 'b' in out
        assert 'name' not in out
        assert 'idx' not in out
        assert 'x' not in out
        assert 'not_present' not in out

    def test_remap_remove(self):

        s = PlainSample(data={'name': 'sample', 'idx': 111})

        stage = StageRemap(remap={'name': 'a'}, remove_missing=True)
        _plug_test(stage)

        out = stage(s)

        assert 'a' in out
        assert 'idx' not in out


class TestStageKeysFilter(object):

    def test_filter(self):

        s = PlainSample(data={'name': 'sample', 'idx': 111, 'tail': 2.2})

        negates = [True, False]
        for negate in negates:
            stage = StageKeysFilter(keys=['name', 'idx'], negate=negate)
            _plug_test(stage)

            out = stage(s)

            assert ('name' in out) if not negate else ('name' not in out)
            assert ('idx' in out) if not negate else ('idx' not in out)
            assert ('tail' in out) if negate else ('tail' not in out)


class TestStageAugmentations(object):

    def test_augmentations(self, toy_dataset_small):

        import albumentations as A

        folder = toy_dataset_small['folder']
        keypoints_format = toy_dataset_small['keypoints_format']
        bboxes_format = toy_dataset_small['bboxes_format']

        reader = UnderfolderReader(folder=folder)

        transform = A.Compose([
            A.HorizontalFlip(p=1),
            A.ShiftScaleRotate(shift_limit=0.2, scale_limit=0.6, rotate_limit=180, p=1.0),
            A.RandomBrightnessContrast(p=1),
        ],
            keypoint_params=A.KeypointParams(format=keypoints_format, remove_invisible=False, angle_in_degrees=False),
            bbox_params=A.BboxParams(format=bboxes_format)
        )

        stage = StageAugmentations(
            transform_cfg=A.to_dict(transform),
            targets={
                'image': 'image',
                'mask': 'mask',
                'inst': 'inst',
                'keypoints': 'keypoints',
                'bboxes': 'bboxes'
            }
        )
        _plug_test(stage)

        for sample in reader:
            out = stage(sample)

            for key in sample.keys():
                assert key in out


class TestStageCompose(object):

    def test_compose(self):

        s = PlainSample(data={'name': 'sample', 'idx': 111, 'float': 2.3, 'tail': True})

        stages = [
            StageIdentity(),
            StageRemap(remap={'name': 'a'}, remove_missing=False),
            StageRemap(remap={'idx': 'b'}, remove_missing=False),
            StageRemap(remap={'float': 'c'}, remove_missing=False),
            StageKeysFilter(keys=['a', 'b', 'c'], negate=False),
            StageIdentity(),
        ]

        stage = StageCompose(stages=stages)
        _plug_test(stage)

        out = stage(s)

        assert 'a' in out
        assert 'b' in out
        assert 'c' in out
        assert 'tail' not in out
