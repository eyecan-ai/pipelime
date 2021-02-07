from pathlib import Path
from pipelime.factories import GenericFactory
from pipelime.sequences.readers.base import BaseReader
from pipelime.sequences.samples import FileSystemSample
from pipelime.filesystem.toolkit import FSToolkit
from schema import Optional, Schema


@GenericFactory.register
class UnderfolderReader(BaseReader):
    DATA_SUBFOLDER = 'data'

    def __init__(self, folder: str, copy_root_files: bool = True) -> None:
        self._folder = Path(folder)
        self._copy_root_files = copy_root_files
        self._datafolder = self._folder / self.DATA_SUBFOLDER
        assert self._datafolder.exists(), f'No data folder found: "{self._datafolder}"'

        # builds tree from subfolder with underscore notation
        self._tree = FSToolkit.tree_from_underscore_notation_files(self._datafolder)
        self._ids = list(sorted(self._tree.keys()))
        self._root_files = [x for x in Path(self._folder).glob('*') if x.is_file()]
        self._root_files = [x for x in self._root_files if not x.name.startswith('.')]
        self._root_data = {}
        for f in self._root_files:
            self._root_data[f.stem] = str(f)  # FSToolkit.load_data(f)

        samples = []
        for idx in range(len(self._ids)):
            data = dict(self._tree[self._ids[idx]])
            if self._copy_root_files:
                data.update(self._root_data)

            sample = FileSystemSample(data_map=data, lazy=True, id=self._ids[idx])
            samples.append(sample)

        super().__init__(samples=samples)

    @classmethod
    def factory_name(cls) -> str:
        return UnderfolderReader.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.factory_name(),
            'options': {
                'folder': str,
                Optional('copy_root_files'): bool
            }
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        cls.factory_schema().validate(d)
        return UnderfolderReader(**d['options'])

    def to_dict(self) -> dict:
        return {
            'type': self.factory_name(),
            'options': {
                'folder': str(self._folder),
                'copy_root_files': self._copy_root_files
            }
        }
