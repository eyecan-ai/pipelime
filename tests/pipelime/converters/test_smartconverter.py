from pathlib import Path
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.samples import FileSystemSample
import shutil
from pipelime.converters.smartconverter import SmartConverter


class TestSmartConverter:
    def test_conversion(self, tmpdir, sample_underfolder_minimnist):

        folder = sample_underfolder_minimnist["folder"]
        dataset = UnderfolderReader(folder=folder)

        output_folder = Path(tmpdir.mkdir("subfolders_dataset"))
        print("Conversion to: ", output_folder)

        for sample in dataset:
            sample: FileSystemSample
            sample_id = sample["metadata"]["sample_id"]
            split_A = int(sample["metadata"]["double"]) % 3
            split_B = int(sample["metadata"]["double"]) % 5
            split_C = "custom  string-to replace"
            sample_path = (
                output_folder
                / str(sample_id)
                / str(split_A)
                / str(split_B)
                / str(split_C)
            )

            print(sample_path)
            if not sample_path.exists():
                sample_path.mkdir(parents=True)

            image_path = sample_path / Path(sample.filesmap["image"]).name
            metadata_path = sample_path / f'{Path(sample.filesmap["image"]).stem}.yml'

            image_path_hidden = sample_path / f".{ Path(sample.filesmap['image']).name}"

            shutil.copy(sample.filesmap["image"], image_path)
            shutil.copy(sample.filesmap["image"], image_path_hidden)
            shutil.copy(sample.filesmap["metadata"], metadata_path)

        output_folder_underfolder = Path(tmpdir.mkdir("underfolder_dataset")) / "empty"
        converter = SmartConverter(
            folder=output_folder, extensions_map={"image": "jpg", "metadata": "yml"}
        )
        converter.convert(output_folder_underfolder)

        reloaded_undefolder = UnderfolderReader(folder=output_folder_underfolder)
        assert len(reloaded_undefolder) == len(dataset)

        print("RELOADED UNDERFODLER:")
        for sample in reloaded_undefolder:
            print(sample[SmartConverter.CONVERTED_METADATA_KEY]["category"])
            original_sample_id = int(
                sample[SmartConverter.CONVERTED_METADATA_KEY]["filename"].split(
                    "_", maxsplit=1
                )[0]
            )
            reloadeed_sample_id = int(
                sample[SmartConverter.CONVERTED_METADATA_KEY]["category"].split("_")[0]
            )
            assert reloadeed_sample_id == original_sample_id
