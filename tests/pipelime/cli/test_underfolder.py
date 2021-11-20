from click.testing import CliRunner

from pipelime.cli.underfolder.operations import operation_split
from pipelime.sequences.readers.filesystem import UnderfolderReader


class TestCLIUnderfolderOperationSum:
    def test_sum(self, tmpdir, sample_underfolder_minimnist):

        from pathlib import Path

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

        import uuid
        from pathlib import Path

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


class TestCLIUnderfolderOperationShuffle:
    def test_shuffle(self, tmpdir, sample_underfolder_minimnist):

        from pathlib import Path

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

        import uuid
        from pathlib import Path

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
            options.extend(["-s", f"{str(k)}", f"{str(v)}"])

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

        import uuid
        from pathlib import Path

        from pipelime.cli.underfolder.operations import operation_filterbyquery

        input_folder = sample_underfolder_minimnist["folder"]
        input_dataset = UnderfolderReader(folder=input_folder)

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

        import uuid
        from pathlib import Path

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

        import uuid
        from pathlib import Path

        from pipelime.cli.underfolder.operations import operation_filterbyscript

        input_folder = sample_underfolder_minimnist["folder"]
        input_dataset = UnderfolderReader(folder=input_folder)

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

        import uuid
        from pathlib import Path

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

        import uuid
        from pathlib import Path

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

        import uuid
        from pathlib import Path

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

        import uuid
        from pathlib import Path

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

        import uuid
        from pathlib import Path

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
            f"-i {input_folder} -o {input_folder_a} -k pose -k label",
        )
        runner.invoke(
            operation_filterkeys,
            f"-i {input_folder} -o {input_folder_b} -k image -k points",
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
