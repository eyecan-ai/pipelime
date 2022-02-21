from choixe.spooks import Spook

from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.writers.filesystem import UnderfolderWriterV2
from pipelime.sequences.readers.base import ReaderTemplate
from pipelime.sequences.samples import PlainSample, SamplesSequence
from pipelime.sequences.stages import (
    SampleStage,
    StageAugmentations,
    StageCompose,
    StageIdentity,
    StageKeysFilter,
    StageRemap,
    RemoteParams,
    StageUploadToRemote,
    StageRemoveRemote,
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


class TestStageRemote:
    def _check_remote_list(self, data_folder, expected_remote_list):
        from pipelime.filesystem.toolkit import FSToolkit
        from urllib.parse import urlparse, ParseResult
        from pathlib import Path

        for x in UnderfolderReader(data_folder):
            for k in x.keys():
                if FSToolkit.is_remote_file(x.metaitem(k).source()):
                    rmlist = FSToolkit.load_remote_list(x.metaitem(k).source())
                    rmlist = [urlparse(rm) for rm in rmlist]
                    rmlist = [
                        ParseResult(
                            scheme=rm.scheme,
                            netloc="localhost",
                            path=Path(rm.path[1:]).parent.as_posix(),
                            params="",
                            query="",
                            fragment="",
                        ).geturl()
                        for rm in rmlist
                    ]

                    assert expected_remote_list == rmlist

    def _upload_to_remote(
        self, dataset, out_folder, remote_prms, filter_fn=None, check_data=True
    ):
        from pipelime.sequences.proxies import FilteredSamplesSequence
        from pipelime.filesystem.toolkit import FSToolkit
        import numpy as np

        # read and upload
        reader = UnderfolderReader(dataset)
        filtered_seq = (
            reader if filter_fn is None else FilteredSamplesSequence(reader, filter_fn)
        )
        sseq = SamplesSequence(
            filtered_seq,
            StageUploadToRemote(remote_prms, {"image": "png", "mask": "png"}),
        )

        # save after uploading
        reader_template = reader.get_reader_template()
        assert isinstance(reader_template, ReaderTemplate)
        reader_template.extensions_map["image"] = "remote"
        reader_template.extensions_map["mask"] = "remote"

        out_folder.mkdir(parents=True)
        writer = UnderfolderWriterV2(
            out_folder,
            copy_mode=UnderfolderWriterV2.CopyMode.HARD_LINK,
            reader_template=reader_template,
        )

        writer(sseq)

        if check_data:
            # high-level check
            reader_out = UnderfolderReader(out_folder)
            for x, y in zip(filtered_seq, reader_out):
                assert FSToolkit.is_remote_file(y.metaitem("image").source())
                assert FSToolkit.is_remote_file(y.metaitem("mask").source())

                assert x.keys() == y.keys()
                for k, v in x.items():
                    if isinstance(v, np.ndarray):
                        assert np.array_equal(v, y[k])
                    else:
                        assert v == y[k]

        return len(sseq)

    def test_file_upload(self, toy_dataset_small, tmp_path):
        # data lake
        remote_root = tmp_path / "remote"
        remote_root.mkdir(parents=True)
        remote_root = remote_root.as_posix()

        self._upload_to_remote(
            toy_dataset_small["folder"],
            tmp_path / "output",
            RemoteParams(scheme="file", netloc="localhost", base_path=remote_root),
        )

    def test_s3_upload(self, toy_dataset_small, tmp_path, minio):
        # data lake
        if not minio:
            from pytest import skip

            skip("MinIO unavailable")

        self._upload_to_remote(
            toy_dataset_small["folder"],
            tmp_path / "output",
            RemoteParams(
                scheme="s3",
                netloc="localhost:9000",
                base_path="test-s3-upload",
                init_args={
                    "access_key": minio,
                    "secret_key": minio,
                    "secure_connection": False,
                },
            ),
        )

    def test_incremental_file_upload(self, toy_dataset_small, tmp_path):
        from pipelime.sequences.proxies import FilteredSamplesSequence
        from pipelime.filesystem.toolkit import FSToolkit
        from shutil import rmtree
        import pytest
        import numpy as np
        from filecmp import cmp

        # data lake
        remote_root = tmp_path / "remote"
        remote_root.mkdir(parents=True)
        remote_root = remote_root.as_posix()

        input_dataset = toy_dataset_small["folder"]
        output_dataset_a = tmp_path / "output_a"

        stage_upload = RemoteParams(
            scheme="file", netloc="localhost", base_path=remote_root
        )

        # upload even samples
        counter_even = self._upload_to_remote(
            input_dataset,
            output_dataset_a,
            stage_upload,
            lambda x: int(x["metadata"]["index"]) % 2 == 0,
        )

        # manually copy the odd samples
        reader = UnderfolderReader(input_dataset, copy_root_files=False)
        sseq = FilteredSamplesSequence(
            reader, lambda x: int(x["metadata"]["index"]) % 2 == 1
        )
        writer = UnderfolderWriterV2(
            output_dataset_a,
            copy_mode=UnderfolderWriterV2.CopyMode.HARD_LINK,
            reader_template=reader.get_reader_template(),
        )
        writer(sseq)

        # clear all remote data
        rmtree(remote_root, ignore_errors=True)
        remote_root = tmp_path / "remote"
        remote_root.mkdir(parents=True, exist_ok=True)
        remote_root = remote_root.as_posix()

        # upload all indices, but only the odd ones are actually copied to the remote
        output_dataset_b = tmp_path / "output_b"
        counter_all = self._upload_to_remote(
            output_dataset_a, output_dataset_b, stage_upload, check_data=False
        )

        reader = UnderfolderReader(output_dataset_b)
        assert counter_even == len(reader) // 2
        assert counter_all == len(reader)

        # only the odd samples are on the remote
        for x, y in zip(UnderfolderReader(output_dataset_a), reader):
            assert x.keys() == y.keys()
            for k in x.keys():
                if k in ("image", "mask") and int(x["metadata"]["index"]) % 2 == 0:
                    # even sample
                    assert FSToolkit.is_remote_file(x.metaitem(k).source())
                    assert FSToolkit.is_remote_file(y.metaitem(k).source())
                    assert cmp(
                        x.metaitem(k).source(), y.metaitem(k).source(), shallow=False
                    )
                    with pytest.raises(Exception):
                        _ = x[k]
                    with pytest.raises(Exception):
                        _ = y[k]
                else:
                    v = x[k]
                    if isinstance(v, np.ndarray):
                        assert np.array_equal(v, y[k])
                    else:
                        assert v == y[k]

    def test_multiple_remote_upload(self, toy_dataset_small, tmp_path):
        from urllib.parse import ParseResult
        from shutil import rmtree
        import numpy as np

        # create two remotes
        remote_a = tmp_path / "remote_a"
        remote_a.mkdir(parents=True, exist_ok=True)
        remote_a = remote_a.as_posix()

        remote_b = tmp_path / "remote_b"
        remote_b.mkdir(parents=True, exist_ok=True)
        remote_b = remote_b.as_posix()

        input_dataset = toy_dataset_small["folder"]
        output_dataset_a = tmp_path / "output_a"

        # upload to both remotes
        self._upload_to_remote(
            input_dataset,
            output_dataset_a,
            [
                RemoteParams(scheme="file", netloc="localhost", base_path=remote_a),
                RemoteParams(scheme="file", netloc="localhost", base_path=remote_b),
            ],
        )

        # the .remote files must contains both remotes, remote_root first
        expected_remote_list = [
            ParseResult(
                scheme="file",
                netloc="localhost",
                path=remote_a,
                params="",
                query="",
                fragment="",
            ).geturl(),
            ParseResult(
                scheme="file",
                netloc="localhost",
                path=remote_b,
                params="",
                query="",
                fragment="",
            ).geturl(),
        ]
        self._check_remote_list(output_dataset_a, expected_remote_list)

        # now remove remote_a and check again the data
        rmtree(remote_a, ignore_errors=True)

        for x, y in zip(
            UnderfolderReader(input_dataset), UnderfolderReader(output_dataset_a)
        ):
            for k, v in x.items():
                if isinstance(v, np.ndarray):
                    assert np.array_equal(v, y[k])
                else:
                    assert v == y[k]

        # now upload to remote_c taking data from remote_b
        remote_c = tmp_path / "remote_c"
        remote_c.mkdir(parents=True, exist_ok=True)
        remote_c = remote_c.as_posix()

        output_dataset_b = tmp_path / "output_b"
        self._upload_to_remote(
            output_dataset_a,
            output_dataset_b,
            RemoteParams(scheme="file", netloc="localhost", base_path=remote_c),
        )

        expected_remote_list.append(
            ParseResult(
                scheme="file",
                netloc="localhost",
                path=remote_c,
                params="",
                query="",
                fragment="",
            ).geturl()
        )

        self._check_remote_list(output_dataset_b, expected_remote_list)

    def test_remove_remote(self, toy_dataset_small, tmp_path):
        from urllib.parse import ParseResult

        # create two remotes
        remote_a = tmp_path / "remote_a"
        remote_a.mkdir(parents=True, exist_ok=True)
        remote_a = remote_a.as_posix()

        remote_b = tmp_path / "remote_b"
        remote_b.mkdir(parents=True, exist_ok=True)
        remote_b = remote_b.as_posix()

        input_dataset = toy_dataset_small["folder"]
        output_dataset_a = tmp_path / "output_a"

        # upload to both remotes
        self._upload_to_remote(
            input_dataset,
            output_dataset_a,
            [
                RemoteParams(scheme="file", netloc="localhost", base_path=remote_a),
                RemoteParams(scheme="file", netloc="localhost", base_path=remote_b),
            ],
        )

        # the .remote files must contains both remotes, remote_root first
        expected_remote_list = [
            ParseResult(
                scheme="file",
                netloc="localhost",
                path=remote_a,
                params="",
                query="",
                fragment="",
            ).geturl(),
            ParseResult(
                scheme="file",
                netloc="localhost",
                path=remote_b,
                params="",
                query="",
                fragment="",
            ).geturl(),
        ]
        self._check_remote_list(output_dataset_a, expected_remote_list)

        # remove remote_a
        reader = UnderfolderReader(output_dataset_a)
        sseq = SamplesSequence(
            reader,
            StageRemoveRemote(
                RemoteParams(scheme="file", netloc="localhost", base_path=remote_a),
                ["image", "mask"],
            ),
        )

        output_dataset_b = tmp_path / "output_b"
        output_dataset_b.mkdir(parents=True)
        writer = UnderfolderWriterV2(
            output_dataset_b,
            copy_mode=UnderfolderWriterV2.CopyMode.HARD_LINK,
            reader_template=reader.get_reader_template(),
        )
        writer(sseq)

        expected_remote_list.pop(0)
        self._check_remote_list(output_dataset_b, expected_remote_list)
