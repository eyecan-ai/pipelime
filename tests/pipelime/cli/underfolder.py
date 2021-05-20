from pipelime.sequences.readers.filesystem import UnderfolderReader
from click.testing import CliRunner


class TestCLIUnderfolderOperations:

    def test_sum(self, tmpdir, sample_underfolder_minimnist):

        from pipelime.cli.underfolder.operations import operation_sum
        from pathlib import Path

        input_folder = sample_underfolder_minimnist['folder']
        input_dataset = UnderfolderReader(folder=input_folder)
        output_folder = Path(tmpdir.mkdir("test_sum"))
        N = 5

        options = []
        for _ in range(N):
            options.extend(['-i', str(input_folder)])
        options.extend(['-o', f'{str(output_folder)}'])

        runner = CliRunner()
        result = runner.invoke(operation_sum, options)
        print(result)
        assert result.exit_code == 0

        output_reader = UnderfolderReader(folder=output_folder, lazy_samples=True)
        print("OUTPUT", output_folder)
        assert len(output_reader) == len(input_dataset) * N

    def test_subsample(self, tmpdir, sample_underfolder_minimnist):

        from pipelime.cli.underfolder.operations import operation_subsample
        from pathlib import Path
        import uuid
        input_folder = sample_underfolder_minimnist['folder']
        input_dataset = UnderfolderReader(folder=input_folder)

        N = 5
        perc = 1 / N
        expected_length = int(len(input_dataset) / N)
        # expected_length_sub = len(input_dataset) // N

        for factor in [perc, N]:
            output_folder = Path(tmpdir.mkdir(str(uuid.uuid1())))

            options = []
            options.extend(['-i', str(input_folder)])
            options.extend(['-o', f'{str(output_folder)}'])
            options.extend(['-f', f'{factor}'])

            runner = CliRunner()
            result = runner.invoke(operation_subsample, options)
            print(result)

            assert result.exit_code == 0
            output_reader = UnderfolderReader(folder=output_folder, lazy_samples=True)
            print("OUTPUT", factor, output_folder)

            assert len(output_reader) == expected_length
