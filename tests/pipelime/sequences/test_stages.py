from choixe.spooks import Spook

from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.samples import PlainSample, SamplesSequence
from pipelime.sequences.stages import (
    SampleStage,
    StageAugmentations,
    StageCompose,
    StageIdentity,
    StageKeysFilter,
    StageRemap,
)


def _plug_test(stage: SampleStage):
    """Test what a generic SampleStage should do

    :param stage: input SampleStage
    :type stage: SampleStage
    """

    assert isinstance(stage, SampleStage)

    restage = stage.from_dict(stage.to_dict())
    assert isinstance(restage, SampleStage)
    schema = restage.spook_schema()
    assert isinstance(schema, dict) or schema is None

    factored = Spook.create(stage.serialize())
    assert isinstance(factored, SampleStage)


class TestStageIdentity(object):
    def test_identity(self):

        s = PlainSample(data={"name": "sample", "idx": 111})

        stage = StageIdentity()
        _plug_test(stage)

        out = stage(s)

        assert out == s


class TestStageRemap(object):
    def test_remap(self):

        s = PlainSample(data={"name": "sample", "idx": 111})

        stage = StageRemap(remap={"name": "a", "idx": "b", "not_present": "x"})
        _plug_test(stage)

        out = stage(s)

        assert "a" in out
        assert "b" in out
        assert "name" not in out
        assert "idx" not in out
        assert "x" not in out
        assert "not_present" not in out

    def test_remap_remove(self):

        s = PlainSample(data={"name": "sample", "idx": 111})

        stage = StageRemap(remap={"name": "a"}, remove_missing=True)
        _plug_test(stage)

        out = stage(s)

        assert "a" in out
        assert "idx" not in out


class TestStageKeysFilter(object):
    def test_filter(self):

        s = PlainSample(data={"name": "sample", "idx": 111, "tail": 2.2})

        negates = [True, False]
        for negate in negates:
            stage = StageKeysFilter(key_list=["name", "idx"], negate=negate)
            _plug_test(stage)

            out = stage(s)

            assert ("name" in out) if not negate else ("name" not in out)
            assert ("idx" in out) if not negate else ("idx" not in out)
            assert ("tail" in out) if negate else ("tail" not in out)


class TestStageAugmentations(object):
    def test_augmentations(self, toy_dataset_small, tmp_path):

        import albumentations as A

        folder = toy_dataset_small["folder"]
        keypoints_format = toy_dataset_small["keypoints_format"]
        bboxes_format = toy_dataset_small["bboxes_format"]

        reader = UnderfolderReader(folder=folder)

        transform = A.Compose(
            [
                A.HorizontalFlip(p=1),
                A.ShiftScaleRotate(
                    shift_limit=0.2, scale_limit=0.6, rotate_limit=180, p=1.0
                ),
                A.RandomBrightnessContrast(p=1),
            ],
            keypoint_params=A.KeypointParams(
                format=keypoints_format, remove_invisible=False, angle_in_degrees=False
            ),
            bbox_params=A.BboxParams(format=bboxes_format),
        )

        stage = StageAugmentations(
            transform_cfg=A.to_dict(transform),
            targets={
                "image": "image",
                "mask": "mask",
                "inst": "inst",
                "keypoints": "keypoints",
                "bboxes": "bboxes",
            },
        )
        _plug_test(stage)

        for sample in reader:
            out = stage(sample)
            for key in sample.keys():
                assert key in out

        transform_file = str(tmp_path / "tr.json")
        A.save(transform, transform_file)
        stage_fromfile = StageAugmentations(
            transform_cfg=transform_file,
            targets={
                "image": "image",
                "mask": "mask",
                "inst": "inst",
                "keypoints": "keypoints",
                "bboxes": "bboxes",
            },
        )
        assert stage.to_dict() == stage_fromfile.to_dict()


class TestStageCompose(object):
    def test_compose(self):

        s = PlainSample(data={"name": "sample", "idx": 111, "float": 2.3, "tail": True})

        stages = [
            StageIdentity(),
            StageRemap(remap={"name": "a"}, remove_missing=False),
            StageRemap(remap={"idx": "b"}, remove_missing=False),
            StageRemap(remap={"float": "c"}, remove_missing=False),
            StageKeysFilter(key_list=["a", "b", "c"], negate=False),
            StageIdentity(),
        ]

        stage = StageCompose(stages=stages)
        _plug_test(stage)

        out = stage(s)

        assert "a" in out
        assert "b" in out
        assert "c" in out
        assert "tail" not in out


class TestSampleSequenceStaged:
    def test_samplessequence_staged(self):

        samples = []
        for index in range(10):
            samples.append(
                PlainSample(
                    data={"name": "sample", "idx": index, "float": 2.3, "tail": True}
                )
            )

        stages = [
            StageIdentity(),
            StageRemap(remap={"name": "a"}, remove_missing=False),
            StageRemap(remap={"idx": "b"}, remove_missing=False),
            StageRemap(remap={"float": "c"}, remove_missing=False),
            StageKeysFilter(key_list=["a", "b"], negate=False),
        ]

        sequence = SamplesSequence(samples=samples)
        sequence.stage = StageCompose(stages=stages)

        for sample in sequence:
            assert "a" in sample
            assert "b" in sample
            assert "c" not in sample
            assert "tail" not in sample
