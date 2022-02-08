from click.testing import CliRunner
import math
import uuid
from pathlib import Path
from pipelime.sequences.readers.filesystem import UnderfolderReader


class TestCLIUnderfolderOperationSum:
    def test_sum(self, tmpdir, sample_underfolder_minimnist):
        from pipelime.cli.underfolder.operations import operation_sum

        input_folder = sample_underfolder_minimnist["folder"]
        input_dataset = UnderfolderReader(folder=input_folder)
        output_folder = Path(tmpdir.mkdir("test_sum"))
        N = 5

        options = []
        for _ in range(N):
            options.extend(["-i", str(input_folder)])
        options.extend(["-o", f"{str(output_folder)}"])

        runner = CliRunner()
        result = runner.invoke(operation_sum, options)
        print(result)
        assert result.exit_code == 0

        output_reader = UnderfolderReader(folder=output_folder, lazy_samples=True)
        print("OUTPUT", output_folder)
        assert len(output_reader) == len(input_dataset) * N


class TestCLIUnderfolderOperationSubsample:
    def test_subsample(self, tmpdir, sample_underfolder_minimnist):
        from pipelime.cli.underfolder.operations import operation_subsample

        input_folder = sample_underfolder_minimnist["folder"]
        input_dataset = UnderfolderReader(folder=input_folder)

        N = 5
        perc = 1 / N
        expected_length = int(len(input_dataset) / N)
        # expected_length_sub = len(input_dataset) // N

        for factor in [perc, N]:
            output_folder = Path(tmpdir.mkdir(str(uuid.uuid1())))

            options = []
            options.extend(["-i", str(input_folder)])
            options.extend(["-o", f"{str(output_folder)}"])
            options.extend(["-f", f"{factor}"])

            runner = CliRunner()
            result = runner.invoke(operation_subsample, options)
            print(result)

            assert result.exit_code == 0
            output_reader = UnderfolderReader(folder=output_folder, lazy_samples=True)
            print("OUTPUT", factor, output_folder)

            assert len(output_reader) == expected_length

    def test_subsample_start(self, tmpdir, sample_underfolder_minimnist):
        from pipelime.cli.underfolder.operations import operation_subsample

        input_folder = sample_underfolder_minimnist["folder"]
        input_dataset = UnderfolderReader(folder=input_folder)
        L = len(input_dataset)

        N = 5
        start_N = 7
        exp_N = math.ceil((L - start_N) / N)
        perc = 1 / N
        start_perc = 0.1
        exp_perc = min(int(L * perc), L - int(L * perc))

        for factor, start, exp_length in zip(
            [perc, N], [start_perc, start_N], [exp_perc, exp_N]
        ):
            output_folder = Path(tmpdir.mkdir(str(uuid.uuid1())))
            options = []
            options.extend(["-i", str(input_folder)])
            options.extend(["-o", f"{str(output_folder)}"])
            options.extend(["-f", f"{factor}"])
            options.extend(["-s", f"{start}"])

            runner = CliRunner()
            result = runner.invoke(operation_subsample, options)
            print(result)

            assert result.exit_code == 0
            output_reader = UnderfolderReader(folder=output_folder, lazy_samples=True)
            print("OUTPUT", factor, output_folder)

            assert len(output_reader) == exp_length


class TestCLIUnderfolderOperationShuffle:
    def test_shuffle(self, tmpdir, sample_underfolder_minimnist):
        from pipelime.cli.underfolder.operations import operation_shuffle

        input_folder = sample_underfolder_minimnist["folder"]
        input_dataset = UnderfolderReader(folder=input_folder)
        output_folder = Path(tmpdir.mkdir("test_shuffle"))

        options = []
        options.extend(["-i", str(input_folder)])
        options.extend(["-o", f"{str(output_folder)}"])
        options.extend(
            ["-s", f"{str(1024)}"]
        )  # This should shuffle dataset such as `7` will be the new first sample! CHeck it!!

        runner = CliRunner()
        result = runner.invoke(operation_shuffle, options)
        print(result)
        assert result.exit_code == 0

        output_reader = UnderfolderReader(folder=output_folder, lazy_samples=True)
        print("OUTPUT", output_folder)
        assert len(output_reader) == len(input_dataset)
        assert output_reader[0]["label"] != input_dataset[0]["label"]


class TestCLIUnderfolderOperationSplit:
    def test_split(self, tmpdir, sample_underfolder_minimnist):
        from pipelime.cli.underfolder.operations import operation_split

        input_folder = sample_underfolder_minimnist["folder"]
        input_dataset = UnderfolderReader(folder=input_folder)

        percs = [0.1, 0.2, 0.3, 0.4]
        output_folders = [
            str(Path(tmpdir.mkdir(str(uuid.uuid1())))) for _ in range(len(percs))
        ]
        splits_map = {k: v for k, v in zip(output_folders, percs)}

        options = []
        options.extend(["-i", str(input_folder)])
        for k, v in splits_map.items():
            options.extend(["-o", f"{str(k)}", "-s", f"{str(v)}"])

        runner = CliRunner()
        result = runner.invoke(operation_split, options)
        print(result)

        assert result.exit_code == 0

        cumulative = 0
        for output_folder in output_folders:
            output_reader = UnderfolderReader(folder=output_folder, lazy_samples=True)
            cumulative += len(output_reader)
        assert len(input_dataset) == cumulative


class TestCLIUnderfolderOperationFilterByQuery:
    def test_filterbyquery(self, tmpdir, sample_underfolder_minimnist):
        from pipelime.cli.underfolder.operations import operation_filterbyquery

        input_folder = sample_underfolder_minimnist["folder"]
        UnderfolderReader(folder=input_folder)

        query = "`metadata.double` <= 10.0 AND `metadata.half` <= 2.5"
        # Samples whose double is <= 10 adn half <= 2.5 are  6! Check it!
        expected_size = 6

        output_folder = str(Path(tmpdir.mkdir(str(uuid.uuid1()))))

        options = []
        options.extend(["-i", str(input_folder)])
        options.extend(["-o", f"{str(output_folder)}"])
        options.extend(["-q", f"{str(query)}"])

        runner = CliRunner()
        result = runner.invoke(operation_filterbyquery, options)
        print(result)

        assert result.exit_code == 0

        output_reader = UnderfolderReader(folder=output_folder, lazy_samples=True)
        assert len(output_reader) == expected_size


class TestCLIUnderfolderOperationSplitByQuery:
    def test_splitbyquery(self, tmpdir, sample_underfolder_minimnist):
        from pipelime.cli.underfolder.operations import operation_splitbyquery

        input_folder = sample_underfolder_minimnist["folder"]
        input_dataset = UnderfolderReader(folder=input_folder)

        query = "`metadata.double` <= 10.0 AND `metadata.half` <= 2.5"

        output_folders = [str(Path(tmpdir.mkdir(str(uuid.uuid1())))) for _ in range(2)]

        options = []
        options.extend(["-i", str(input_folder)])
        options.extend(["-o1", f"{str(output_folders[0])}"])
        options.extend(["-o2", f"{str(output_folders[1])}"])
        options.extend(["-q", f"{str(query)}"])

        runner = CliRunner()
        result = runner.invoke(operation_splitbyquery, options)
        print(result)

        assert result.exit_code == 0

        cumulative = 0
        for output_folder in output_folders:
            output_reader = UnderfolderReader(folder=output_folder, lazy_samples=True)
            cumulative += len(output_reader)
        assert cumulative == len(input_dataset)


class TestCLIUnderfolderOperationFilterByScript:
    def test_filterbyscript(self, tmpdir, sample_underfolder_minimnist):
        from pipelime.cli.underfolder.operations import operation_filterbyscript

        input_folder = sample_underfolder_minimnist["folder"]
        UnderfolderReader(folder=input_folder)

        # script
        func = ""
        func += "import numpy as np\n"
        func += "def check_sample(sample, sequence):\n"
        func += " return np.array(sample['metadata']['double']) > 4\n"
        script_path = tmpdir.join("custom_script.py")
        with open(script_path, "w") as f:
            f.write(func)

        # Samples whose double is > 4 17! Check it!
        expected_size = 17

        output_folder = str(Path(tmpdir.mkdir(str(uuid.uuid1()))))

        options = []
        options.extend(["-i", str(input_folder)])
        options.extend(["-o", f"{str(output_folder)}"])
        options.extend(["-s", f"{str(script_path)}"])

        runner = CliRunner()
        result = runner.invoke(operation_filterbyscript, options)
        print(result)

        assert result.exit_code == 0

        output_reader = UnderfolderReader(folder=output_folder, lazy_samples=True)
        assert len(output_reader) == expected_size


class TestCLIUnderfolderOperationFilterKeys:
    def test_filterkeys(self, tmpdir, sample_underfolder_minimnist):
        from pipelime.cli.underfolder.operations import operation_filterkeys

        input_folder = sample_underfolder_minimnist["folder"]
        input_dataset = UnderfolderReader(folder=input_folder)

        # script
        filtering_keys = ["image", "label", "metadata"]

        for negate in [False, True]:
            output_folder = str(Path(tmpdir.mkdir(str(uuid.uuid1()))))

            options = []
            options.extend(["-i", str(input_folder)])
            for k in filtering_keys:
                options.extend(["-k", f"{str(k)}"])
            options.extend(["-o", f"{str(output_folder)}"])
            if negate:
                options.extend(["--negate"])

            runner = CliRunner()
            result = runner.invoke(operation_filterkeys, options)
            print(result)

            assert result.exit_code == 0

            output_reader = UnderfolderReader(folder=output_folder, lazy_samples=True)
            assert len(output_reader) == len(input_dataset)

            for sample in output_reader:
                for key in sample.keys():
                    if negate:
                        assert key not in filtering_keys
                    else:
                        assert key in filtering_keys
                break


class TestCLIUnderfolderOperationOrderKeys:
    def test_orderkeys(self, tmpdir, sample_underfolder_minimnist):
        from pipelime.cli.underfolder.operations import operation_orderby

        input_folder = sample_underfolder_minimnist["folder"]
        input_dataset = UnderfolderReader(folder=input_folder)

        # script
        filtering_keys = ["-metadata.sample_id"]

        output_folder = str(Path(tmpdir.mkdir(str(uuid.uuid1()))))
        print("OUTPUT", output_folder)

        options = []
        options.extend(["-i", str(input_folder)])
        for k in filtering_keys:
            options.extend(["-k", f"{str(k)}"])
        options.extend(["-o", f"{str(output_folder)}"])

        runner = CliRunner()
        result = runner.invoke(operation_orderby, options)
        print(result)

        assert result.exit_code == 0

        output_reader = UnderfolderReader(folder=output_folder, lazy_samples=True)
        assert len(output_reader) == len(input_dataset)

        assert (
            output_reader[0]["label"] == 9
        )  # The first sample should be `9`! CHeck it!!


class TestCLIUnderfolderOperationSplitByValue:
    def test_split_by_value(self, tmpdir, sample_underfolder_minimnist):
        from pipelime.cli.underfolder.operations import operation_split_by_value

        input_folder = sample_underfolder_minimnist["folder"]
        input_dataset = UnderfolderReader(folder=input_folder)

        # script
        split_key = "metadata.sample_id"

        output_folder = Path(tmpdir.mkdir(str(uuid.uuid1())))
        print("OUTPUT", output_folder)

        options = []
        options.extend(["-i", str(input_folder)])
        options.extend(["-k", f"{str(split_key)}"])
        options.extend(["-o", f"{str(output_folder)}"])

        runner = CliRunner()
        result = runner.invoke(operation_split_by_value, options)
        print(result)

        assert result.exit_code == 0

        total = 0
        for subfolder in output_folder.iterdir():
            print(subfolder)
            output_reader = UnderfolderReader(folder=subfolder)
            total += len(output_reader)
        assert total == len(input_dataset)


class TestCLIUnderfolderOperationGroupBy:
    def test_groupby(self, tmpdir, sample_underfolder_minimnist):
        from pipelime.cli.underfolder.operations import operation_groupby

        input_folder = sample_underfolder_minimnist["folder"]
        input_dataset = UnderfolderReader(folder=input_folder)

        # script
        group_key = "metadata.sample_id"

        output_folder = str(Path(tmpdir.mkdir(str(uuid.uuid1()))))
        print("OUTPUT", output_folder)

        options = []
        options.extend(["-i", str(input_folder)])
        options.extend(["-k", f"{str(group_key)}"])
        options.extend(["-o", f"{str(output_folder)}"])

        runner = CliRunner()
        result = runner.invoke(operation_groupby, options)
        print(result)

        assert result.exit_code == 0

        output_reader = UnderfolderReader(folder=output_folder, lazy_samples=True)
        assert len(output_reader) == len(
            input_dataset
        )  # TODO: this test is pretty useless! check content!


class TestCLIUnderfolderOperationMix:
    def test_mix(self, tmpdir, sample_underfolder_minimnist):
        from pipelime.cli.underfolder.operations import (
            operation_filterkeys,
            operation_mix,
        )

        input_folder = sample_underfolder_minimnist["folder"]
        input_dataset = UnderfolderReader(folder=input_folder)
        input_folder_a = str(Path(tmpdir.mkdir(str(uuid.uuid1()))))
        input_folder_b = str(Path(tmpdir.mkdir(str(uuid.uuid1()))))
        output_folder = str(Path(tmpdir.mkdir(str(uuid.uuid1()))))
        print(set(input_dataset[0].keys()))
        runner = CliRunner()
        runner.invoke(
            operation_filterkeys,
            f'-i "{input_folder}" -o "{input_folder_a}" -k pose -k label',
        )
        runner.invoke(
            operation_filterkeys,
            f'-i "{input_folder}" -o "{input_folder_b}" -k image -k points',
        )

        options = []
        options.extend(["-i", str(input_folder_a)])
        options.extend(["-i", str(input_folder_b)])
        options.extend(["-o", f"{str(output_folder)}"])

        res = runner.invoke(operation_mix, options)
        assert res.exit_code == 0

        output_reader = UnderfolderReader(folder=output_folder)
        assert len(output_reader) == len(input_dataset)
        keys = set(output_reader[0].keys())
        assert keys == {"pose", "label", "image", "points"}


class TestCLIUnderfolderOperationRemapKeys:
    def test_remapkeys(self, tmpdir, sample_underfolder_minimnist):
        from pipelime.cli.underfolder.operations import operation_remap_keys

        input_folder = sample_underfolder_minimnist["folder"]
        input_reader = UnderfolderReader(folder=input_folder)

        for remove_missing in [True, False]:
            keys = {"metadata": "newmetadata", "image": "newimage"}

            # Samples whose double is <= 10 adn half <= 2.5 are  6! Check it!
            # expected_size = 6

            output_folder = str(Path(tmpdir.mkdir(str(uuid.uuid1()))))

            options = []
            options.extend(["-i", str(input_folder)])
            options.extend(["-o", f"{str(output_folder)}"])
            for k0, k1 in keys.items():
                options.extend(["-k", k0, k1])
            if remove_missing:
                options.extend(["-R"])

            runner = CliRunner()
            result = runner.invoke(operation_remap_keys, options)
            print(result)

            assert result.exit_code == 0

            output_reader = UnderfolderReader(folder=output_folder, lazy_samples=True)
            assert len(output_reader) == len(input_reader)
            for sample in output_reader:
                for k0, k1 in keys.items():
                    assert k0 not in sample.keys()
                    assert k1 in sample.keys()
                if remove_missing:
                    assert len(sample.keys()) == len(keys)
                else:
                    assert len(sample.keys()) > len(keys)


class TestCLIUnderfolderOperationUpload:
    def _recursive_folder_diff(self, folder_a, folder_b):
        import filecmp

        def _diff_files(dcmp: filecmp.dircmp):
            assert len(dcmp.left_only) == 0
            assert len(dcmp.right_only) == 0
            for name in dcmp.common_files:
                assert filecmp.cmp(
                    str(Path(dcmp.left) / name),
                    str(Path(dcmp.right) / name),
                    shallow=False,
                )
            for sub_dcmp in dcmp.subdirs.values():
                _diff_files(sub_dcmp)

        _diff_files(filecmp.dircmp(folder_a, folder_b))

    def _cli_upload(self, input_folder, output_folder, url_list):
        from pipelime.cli.underfolder.operations import upload_to_remote

        keys_to_upload = ["numbers", "image", "image_mask", "metadata", "metadatay"]

        # run CLI
        options = (
            [
                "-i",
                input_folder,
                "-o",
                output_folder,
                "--hardlink",
            ]
            + [r for u in url_list for r in ["-r", u]]
            + [a for k in keys_to_upload for a in ["-k", k]]
        )

        runner = CliRunner()
        result = runner.invoke(upload_to_remote, options)
        assert result.exit_code == 0

        return keys_to_upload

    def _compare_dataset(self, folder_a, folder_b, uploaded_keys):
        # get data back
        import numpy as np

        input_reader = UnderfolderReader(folder_a)
        output_reader = UnderfolderReader(folder_b)
        for x, y in zip(input_reader, output_reader):
            for k in uploaded_keys:
                assert y.metaitem(k).source().suffix == ".remote"

            assert x.keys() == y.keys()
            for k, v in x.items():
                if isinstance(v, np.ndarray):
                    assert np.array_equal(v, y[k])
                else:
                    assert v == y[k]

    def test_cli_upload_to_file_remote(self, tmp_path, sample_underfolder_minimnist):
        from urllib.parse import ParseResult

        # input/output
        input_folder = str(sample_underfolder_minimnist["folder"])

        output_folder = tmp_path / "output"
        output_folder.mkdir(parents=True)
        output_folder = str(output_folder)

        # data lakes
        remote_root_a = tmp_path / "remote_a"
        remote_root_a.mkdir(parents=True)
        remote_root_a = remote_root_a.as_posix()

        remote_root_b = tmp_path / "remote_b"
        remote_root_b.mkdir(parents=True)
        remote_root_b = remote_root_b.as_posix()

        uploaded_keys = self._cli_upload(
            input_folder,
            output_folder,
            [
                ParseResult(
                    scheme="file",
                    netloc="localhost",
                    path=remote_root_a,
                    params="",
                    query="",
                    fragment="",
                ).geturl(),
                ParseResult(
                    scheme="file",
                    netloc="",
                    path=remote_root_b,
                    params="",
                    query="",
                    fragment="",
                ).geturl(),
            ],
        )

        # check remote data
        self._recursive_folder_diff(remote_root_a, remote_root_b)

        # check local data
        self._compare_dataset(input_folder, output_folder, uploaded_keys)

        # make remote_a unavailable and repeat
        Path(remote_root_a).rename(remote_root_a + "_")
        self._compare_dataset(input_folder, output_folder, uploaded_keys)

        # make remote_b unavailable and repeat
        Path(remote_root_a + "_").rename(remote_root_a)
        Path(remote_root_b).rename(remote_root_b + "_")
        self._compare_dataset(input_folder, output_folder, uploaded_keys)

        # make both unavailable and repeat
        import pytest

        Path(remote_root_a).rename(remote_root_a + "_")
        with pytest.raises(Exception):
            self._compare_dataset(input_folder, output_folder, uploaded_keys)

    def test_cli_upload_to_s3_remote(
        self, tmp_path, sample_underfolder_minimnist, minio
    ):
        if not minio:
            from pytest import skip

            skip("MinIO unavailable")

        from urllib.parse import ParseResult

        # input/output
        input_folder = str(sample_underfolder_minimnist["folder"])

        output_folder = tmp_path / "output"
        output_folder.mkdir(parents=True)
        output_folder = str(output_folder)

        # data lakes
        s3_init_args = {
            "access_key": f"{minio}",
            "secret_key": f"{minio}",
            "secure_connection": False,
        }

        remote_a_args = {
            "scheme": "s3",
            "netloc": "localhost:9000",
            "path": "remote-a",
            "params": "",
            "query": ":".join([k + "=" + str(v) for k, v in s3_init_args.items()]),
            "fragment": "",
        }
        remote_b_args = remote_a_args.copy()
        remote_b_args["path"] = "remote-b"

        uploaded_keys = self._cli_upload(
            input_folder,
            output_folder,
            [
                ParseResult(**remote_a_args).geturl(),
                ParseResult(**remote_b_args).geturl(),
            ],
        )

        # get the remote client
        import pipelime.filesystem.remotes as plr
        from minio import Minio
        from minio.commonconfig import CopySource

        s3_remote = plr.create_remote(
            remote_a_args["scheme"], remote_a_args["netloc"], **s3_init_args
        )
        assert isinstance(s3_remote, plr.S3Remote)

        minio_client = s3_remote.client
        assert isinstance(minio_client, Minio)

        assert minio_client.bucket_exists("remote-a")
        assert minio_client.bucket_exists("remote-b")

        minio_client.make_bucket("tmpbkt-a")
        minio_client.make_bucket("tmpbkt-b")

        # download data from buckets and compare
        def _download_bucket(bkt_name, out_folder):
            for obj in minio_client.list_objects(bkt_name):
                minio_client.fget_object(
                    bkt_name, obj.object_name, str(out_folder / obj.object_name)
                )

        dwld_remote_a = tmp_path / "dwld_remote_a"
        dwld_remote_a.mkdir(parents=True)
        _download_bucket("remote-a", dwld_remote_a)

        dwld_remote_b = tmp_path / "dwld_remote_b"
        dwld_remote_b.mkdir(parents=True)
        _download_bucket("remote-b", dwld_remote_b)

        self._recursive_folder_diff(str(dwld_remote_a), str(dwld_remote_b))

        # check local data
        self._compare_dataset(input_folder, output_folder, uploaded_keys)

        # make remote_a unavailable and repeat
        def _move_objects(source_bkt, target_bkt):
            objlist = []
            for obj in minio_client.list_objects(source_bkt):
                minio_client.copy_object(
                    target_bkt, obj.object_name, CopySource(source_bkt, obj.object_name)
                )
                objlist.append(obj.object_name)
            for obj in objlist:
                minio_client.remove_object(source_bkt, obj)

        _move_objects("remote-a", "tmpbkt-a")
        self._compare_dataset(input_folder, output_folder, uploaded_keys)

        # make remote_b unavailable and repeat
        _move_objects("tmpbkt-a", "remote-a")
        _move_objects("remote-b", "tmpbkt-b")
        self._compare_dataset(input_folder, output_folder, uploaded_keys)

        # make both unavailable and repeat
        import pytest

        _move_objects("remote-a", "tmpbkt-a")
        with pytest.raises(Exception):
            self._compare_dataset(input_folder, output_folder, uploaded_keys)

    def test_cli_remove_remote(self, tmp_path, sample_underfolder_minimnist):
        from pipelime.cli.underfolder.operations import remove_remote
        from urllib.parse import ParseResult

        # input/output
        input_folder = str(sample_underfolder_minimnist["folder"])

        output_folder_a = tmp_path / "output_a"
        output_folder_a.mkdir(parents=True)
        output_folder_a = str(output_folder_a)

        output_folder_b = tmp_path / "output_b"
        output_folder_b.mkdir(parents=True)
        output_folder_b = str(output_folder_b)

        # data lakes
        remote_root_a = tmp_path / "remote_a"
        remote_root_a.mkdir(parents=True)
        remote_root_a = remote_root_a.as_posix()

        remote_root_b = tmp_path / "remote_b"
        remote_root_b.mkdir(parents=True)
        remote_root_b = remote_root_b.as_posix()

        remote_urls = [
            ParseResult(
                scheme="file",
                netloc="localhost",
                path=remote_root_a,
                params="",
                query="",
                fragment="",
            ).geturl(),
            ParseResult(
                scheme="file",
                netloc="",
                path=remote_root_b,
                params="",
                query="",
                fragment="",
            ).geturl(),
        ]

        uploaded_keys = self._cli_upload(
            input_folder,
            output_folder_a,
            remote_urls,
        )

        # remove remote_root_a
        options = (
            [
                "-i",
                output_folder_a,
                "-o",
                output_folder_b,
                "--hardlink",
                "-r",
                remote_urls[0],
            ]
            + [a for k in uploaded_keys for a in ["-k", k]]
        )

        runner = CliRunner()
        result = runner.invoke(remove_remote, options)
        assert result.exit_code == 0

        # check local data
        self._compare_dataset(input_folder, output_folder_b, uploaded_keys)

        # make remote unavailable
        Path(remote_root_b).rename(remote_root_b + "_")
        import pytest

        with pytest.raises(Exception):
            self._compare_dataset(input_folder, output_folder_b, uploaded_keys)
