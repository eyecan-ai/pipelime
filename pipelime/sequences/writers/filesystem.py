from pathlib import Path
from pipelime.sequences.writers.base import BaseWriter
from pipelime.factories import GenericFactory
from pipelime.sequences.readers.base import BaseReader
from pipelime.sequences.samples import FileSystemSample, Sample, SamplesSequence
from pipelime.filesystem.toolkit import FSToolkit
from schema import Optional, Schema


@GenericFactory.register
class UnderfolderWriter(BaseWriter):
    DATA_SUBFOLDER = 'data'

    def __init__(
        self,
        folder: str,
        root_files_keys: list = None,
        extensions_map: dict = None,
        zfill: int = 5
    ) -> None:
        self._folder = Path(folder)
        self._root_files_keys = root_files_keys if root_files_keys is not None else []
        self._extensions_map = extensions_map if extensions_map is not None else {}
        self._zfill = zfill
        self._datafolder = self._folder / self.DATA_SUBFOLDER
        if not self._datafolder.exists():
            self._datafolder.mkdir(parents=True, exist_ok=True)

    def _build_sample_basename(self, sample: Sample):
        if isinstance(sample.id, int):
            return str(sample.id).zfill(self._zfill)
        else:
            return str(sample.id)

    def _build_item_extension(self, key: str):
        if key in self._extensions_map:
            return self._extensions_map[key]
        else:
            return 'pkl'

    def __call__(self, x: SamplesSequence) -> None:

        for sample in x:
            basename = self._build_sample_basename(sample)
            for key in sample.keys():
                itemname = f'{basename}_{key}.{self._build_item_extension(key)}'
                print("writing", basename, itemname)

    @classmethod
    def factory_name(cls) -> str:
        return UnderfolderWriter.__name__

    @classmethod
    def factory_schema(cls) -> Schema:
        return Schema({
            'type': cls.factory_name(),
            'options': {
                'folder': str,
                Optional('root_files_keys'): dict
            }
        })

    @classmethod
    def build_from_dict(cls, d: dict):
        cls.factory_schema().validate(d)
        return UnderfolderWriter(**d['options'])

    def to_dict(self) -> dict:
        return {
            'type': self.factory_name(),
            'options': {
                'folder': str(self._folder),
                'root_files_keys': self._root_files_keys
            }
        }
