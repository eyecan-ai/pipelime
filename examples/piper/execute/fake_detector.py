import click
from pipelime.pipes.piper import Piper, PiperCommand
from pipelime.tools.progress import pipelime_track


@click.command("fake_detector")
@click.option(
    "-i",
    "--input_folder",
    type=click.Path(exists=True),
    required=True,
    help="The input folder",
)
@click.option(
    "-o",
    "--output_folder",
    type=click.Path(),
    required=True,
    help="The output folder",
)
@click.option(
    "-t",
    "--fake_time",
    type=float,
    default=0.01,
    help="Fake delay time",
)
@Piper.command(
    inputs=["input_folder"],
    outputs=["outuput_folder"],
)
def fake_detector(
    input_folder: str,
    output_folder: str,
    fake_time: float,
):

    from pipelime.sequences.readers.filesystem import UnderfolderReader
    from pipelime.sequences.writers.filesystem import UnderfolderWriterV2
    from pipelime.sequences.samples import SamplesSequence
    import time
    import numpy as np

    reader = UnderfolderReader(input_folder)

    # ADD fake detections to each sample
    out_samples = []
    for sample in pipelime_track(
        reader,
        # This progess callback has as CHUNK_ID=0 on a total of 2. Beacause this
        # should map the first part of a global progress bar associated to this node.
        track_callback=PiperCommand.instance.generate_progress_callback(0, 2),
    ):
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
    try:
        writer = UnderfolderWriterV2(
            output_folder,
            reader_template=template,
            file_handling=UnderfolderWriterV2.FileHandling.COPY_IF_NOT_CACHED,
            copy_mode=UnderfolderWriterV2.CopyMode.HARD_LINK,
            # This progess callback has as CHUNK_ID =1 on a total of 2. Beacause this
            # should map the second part of a global progress bar associated to this node.
            progress_callback=PiperCommand.instance.generate_progress_callback(1, 2),
        )
        writer(out_sequence)
    except Exception as e:
        raise click.UsageError(str(e))


if __name__ == "__main__":
    fake_detector()
