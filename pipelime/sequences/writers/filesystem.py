from pipelime.sequences.readers.filesystem import UnderfolderReader
from rich.progress import track
from pipelime.filesystem.toolkit import FSToolkit
import re
from pathlib import Path
from pipelime.factories import Bean, BeanFactory
from pipelime.sequences.writers.base import BaseWriter
from pipelime.sequences.samples import Sample, SamplesSequence
from schema import Optional, Or


class UnderfolderWriter(BaseWriter):
    DATA_SUBFOLDER = 'data'
    DEFAULT_EXTENSION = 'pkl'

    def __init__(
        self,
        folder: str,
        root_files_keys: list = None,
        extensions_map: dict = None,
        zfill: int = 5
    ) -> None:
        self._folder = Path(folder)
        self._empty_template = root_files_keys is None and extensions_map is None
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
        extension = UnderfolderWriter.DEFAULT_EXTENSION
        for r, ext in self._extensions_map.items():
            if re.match(r, key) is not None:
                extension = ext
                break
        return extension

    def _is_root_key(self, key: str):
        for r in self._root_files_keys:
            if re.match(r, key) is not None:
                return True
        return False

    def __call__(self, x: SamplesSequence) -> None:

        if isinstance(x, UnderfolderReader) and self._empty_template:
            template = x.get_filesystem_template()
            if template is not None:
                self._extensions_map = template.extensions_map
                self._root_files_keys = list(template.root_files_keys)
                self._zfill = template.idx_length

        for sample in track(x):
            basename = self._build_sample_basename(sample)
            for key in sample.keys():
                if self._is_root_key(key):
                    itemname = f'{key}.{self._build_item_extension(key)}'
                    output_file = Path(self._folder) / itemname
                    FSToolkit.store_data(output_file, sample[key])
                else:
                    itemname = f'{basename}_{key}.{self._build_item_extension(key)}'
                    output_file = Path(self._datafolder) / itemname
                    FSToolkit.store_data(output_file, sample[key])

    @classmethod
    def bean_schema(cls) -> dict:
        return {
            'folder': str,
            Optional('root_files_keys'): Or(None, list),
            Optional('extensions_map'): Or(None, dict),
            Optional('zfill'): int,
        }

    @classmethod
    def from_dict(cls, d: dict):
        return UnderfolderWriter(
            folder=d.get('folder'),
            root_files_keys=d.get('root_files_keys', None),
            extensions_map=d.get('extensions_map', None),
            zfill=d.get('zfill', 5),
        )

    def to_dict(self) -> dict:
        return {
            'folder': str(self._folder),
            'root_files_keys': self._root_files_keys,
            'extensions_map': self._extensions_map,
            'zfill': self._zfill
        }


BeanFactory.register_bean(UnderfolderWriter)
