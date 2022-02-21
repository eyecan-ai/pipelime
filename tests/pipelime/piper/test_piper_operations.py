from pipelime.pipes.piper import Piper
import pytest
from pathlib import Path
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.writers.filesystem import UnderfolderWriterV2
from pipelime.pipes.piper import Piper, PiperCommand
import click
from click.testing import CliRunner


class TestPiperOperations:
    def test_operations_infos(self):

        operations = [
            {"command": "pipelime underfolder remap_keys", "valid": True},
            {"command": "pipelime underfolder filter_by_query", "valid": True},
            {"command": "pipelime underfolder filter_by_script", "valid": True},
            {"command": "pipelime underfolder filter_keys", "valid": True},
            {"command": "pipelime underfolder group_by", "valid": True},
            {"command": "pipelime underfolder mix", "valid": True},
            {"command": "pipelime underfolder order_by", "valid": True},
            {"command": "pipelime underfolder shuffle", "valid": True},
            {"command": "pipelime underfolder split", "valid": True},
            {"command": "pipelime underfolder split_by_query", "valid": True},
            {
                "command": "pipelime underfolder split_by_value",
                "valid": False,
            },  # ## INVALID PIPER
            {"command": "pipelime underfolder subsample", "valid": True},
            {"command": "pipelime underfolder sum", "valid": True},
            {
                "command": "pipelime underfolder summary",
                "valid": False,
            },  # ## INVALID PIPER
        ]
        for op in operations:

            if op["valid"]:
                description = Piper.piper_command_description(op["command"])
                assert isinstance(description, dict)
            else:
                with pytest.raises(TypeError):
                    Piper.piper_command_description(op["command"])


class TestCLIUnderfolderOperationSplitPiper:
    def test_split(self, tmpdir, sample_underfolder_minimnist):
        import uuid
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
        options.extend([Piper.piper_token_argument(), "token"])

        runner = CliRunner()
        result = runner.invoke(operation_split, options)
        print(result)

        assert result.exit_code == 0

        cumulative = 0
        for output_folder in output_folders:
            output_reader = UnderfolderReader(folder=output_folder, lazy_samples=True)
            cumulative += len(output_reader)
        assert len(input_dataset) == cumulative


class TestCLIUnderfolderOperationSumPiper:
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
        options.extend([Piper.piper_token_argument(), "token"])

        runner = CliRunner()
        result = runner.invoke(operation_sum, options)
        print(result)
        assert result.exit_code == 0

        output_reader = UnderfolderReader(folder=output_folder, lazy_samples=True)
        print("OUTPUT", output_folder)
        assert len(output_reader) == len(input_dataset) * N

# Simple piper command to test multiprocessing
@click.command("piper_cmd_test")
@click.option("-i", "--input_folder", type=Path, required=True)
@click.option("-o", "--output_folder", type=Path, required=True)
@click.option("-w", "--workers", default=0, type=int)
@Piper.command(inputs=["input_folder"], outputs=["output_folder"])
def piper_cmd_test(input_folder: Path, output_folder: Path, workers: int):
    # Read a dataset
    dataset = UnderfolderReader(str(input_folder))

    # Write the same dataset elsewhere
    UnderfolderWriterV2(
        str(output_folder),
        reader_template=dataset.get_reader_template(),
        num_workers=workers,
        progress_callback=PiperCommand.instance.generate_progress_callback(0, 1),
    )(dataset)


class TestPiperMultiprocessing:
    @pytest.mark.parametrize("workers", (0, 1, 2, 4, -1))
    def test_multiprocessing(self, tmp_path, sample_underfolder_minimnist, workers):
        output_folder = tmp_path / "output"
        output_folder.mkdir(parents=True)
        output_folder = str(output_folder)

        options = [
            "-i", sample_underfolder_minimnist["folder"],
            "-o", output_folder,
            "-w", workers,
            Piper.piper_token_argument(), "token"
        ]
        runner = CliRunner()
        result = runner.invoke(piper_cmd_test, options)
        assert result.exit_code == 0

        input_dataset = UnderfolderReader(folder=sample_underfolder_minimnist["folder"])
        output_reader = UnderfolderReader(folder=output_folder)
        assert len(output_reader) == len(input_dataset)
