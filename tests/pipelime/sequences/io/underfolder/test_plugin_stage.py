from pipelime.sequences.readers.filesystem import (
    UnderfolderReader,
    UnderfolderStagePlugin,
)
from pipelime.sequences.stages import (
    StageCompose,
    StageIdentity,
    StageKeysFilter,
    StageRemap,
)
from pathlib import Path
from pipelime.sequences.writers.filesystem import UnderfolderWriter
import rich


class TestUnderfolderStagePlugin:
    def test_stages(self, tmpdir, plain_samples_sequence_generator):

        N = 32
        samples = plain_samples_sequence_generator("", N)

        stages = [
            StageIdentity(),
            StageRemap(remap={"idx": "a"}, remove_missing=False),
            StageRemap(remap={"number": "b"}, remove_missing=False),
            StageRemap(remap={"reverse_number": "c"}, remove_missing=False),
            StageRemap(remap={"odd": "d"}, remove_missing=False),
            StageKeysFilter(key_list=["a", "b"], negate=False),
        ]
        stage = StageCompose(stages=stages)

        output_folder = Path(tmpdir.mkdir("staged_underfolder"))
        UnderfolderWriter(folder=output_folder)(samples)
        UnderfolderStagePlugin.set_stages(source_folder=output_folder, stage=stage)

        rich.print("Output written to:", output_folder)
        reader = UnderfolderReader(folder=output_folder)
        for sample in reader:
            rich.print(list(sample.keys()))
            assert "a" in sample
            assert "b" in sample
            assert "c" not in sample
            assert "d" not in sample
