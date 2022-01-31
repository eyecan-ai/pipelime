import click
from pipelime.pipes.piper import Piper, PiperCommand


@click.command("fake_detector")
@click.option(
    "-i",
    "--input_folders",
    type=click.Path(exists=True),
    required=True,
    multiple=True,
    help="The input folder",
)
@click.option(
    "-o",
    "--output_folders",
    type=click.Path(),
    required=True,
    multiple=True,
    help="The input folder",
)
@click.option(
    "-t",
    "--fake_time",
    type=float,
    default=0.01,
    help="Fake delay time",
)
@Piper.command(
    inputs=["input_folders"],
    outputs=["outuput_folder"],
)
def fake_detector(
    input_folders: str,
    output_folders: str,
    fake_time: float,
    **piper_kwargs,
):

    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriterV2
    from pipelime.sequences.samples import SamplesSequence
    import time
    import numpy as np

    PiperCommand()

    # checks size of input/output folders
    if len(input_folders) != len(output_folders):
        raise click.UsageError(
            "The number of input folders must be equal to the number of output folders"
        )

    for index, input_folder in enumerate(input_folders):

        reader = UnderfolderReader(input_folder)

        # ADD fake detections to each sample
        out_samples = []
        for sample in reader:
            sample["fake_detection"] = {
                "keypoints": [np.random.randint(0, 100, (10, 4)).tolist()]
            }
            out_samples.append(sample)
            time.sleep(fake_time)

        # build out sequence
        out_sequence = SamplesSequence(out_samples)

        # add fake detections to the extensions map
        template = reader.get_reader_template()
        template.extensions_map.update({"fake_detection": "yml"})

        # write out sequence
        writer = UnderfolderWriterV2(
            output_folders[index],
            reader_template=template,
            file_handling=UnderfolderWriterV2.FileHandling.COPY_IF_NOT_CACHED,
            copy_mode=UnderfolderWriterV2.CopyMode.HARD_LINK,
            progress_callback=PiperCommand().generate_progress_callback(
                chunk_index=index, total_chunks=len(input_folders)
            ),
        )
        writer(out_sequence)


if __name__ == "__main__":
    fake_detector()
