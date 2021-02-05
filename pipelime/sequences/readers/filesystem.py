from pathlib import Path
from pipelime.sequences.samples import FileSystemSample, SamplesSequence
from pipelime.filesystem.toolkit import FSToolkit
import rich


class UnderfolderReader(SamplesSequence):
    DATA_SUBFOLDER = 'data'

    def __init__(self, folder: str, copy_root_files: bool = True) -> None:
        self._folder = Path(folder)
        self._datafolder = self._folder / self.DATA_SUBFOLDER
        assert self._datafolder.exists(), f'No data folder found: "{self._datafolder}"'

        # builds tree from subfolder with underscore notation
        self._tree = FSToolkit.tree_from_underscore_notation_files(self._datafolder)
        self._ids = list(sorted(self._tree.keys()))
        self._root_files = [x for x in Path(self._folder).glob('*') if x.is_file()]
        self._root_data = {}
        for f in self._root_files:
            self._root_data[f.stem] = str(f)  # FSToolkit.load_data(f)

        samples = []
        for idx in range(len(self._ids)):
            data = dict(self._tree[self._ids[idx]])
            if copy_root_files:
                data.update(self._root_data)

            sample = FileSystemSample(data_map=data, lazy=True, id=self._ids[idx])
            samples.append(sample)

        super().__init__(samples=samples)
