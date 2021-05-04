from pipelime.sequences.readers.filesystem import UnderfolderReader
from rich.progress import track
from pipelime.filesystem.toolkit import FSToolkit
import re
from pathlib import Path
from pipelime.factories import Bean, BeanFactory
from pipelime.sequences.writers.base import BaseWriter
from pipelime.sequences.samples import FileSystemSample, FilesystemItem, Sample, SamplesSequence
from schema import Optional, Or
import shutil
import os


class UnderfolderWriter(BaseWriter):
    DATA_SUBFOLDER = 'data'
    DEFAULT_EXTENSION = 'pkl'

    def __init__(
        self,
        folder: str,
        root_files_keys: list = None,
        extensions_map: dict = None,
        zfill: int = 5,
        copy_files: bool = True,
        use_symlinks: bool = False
    ) -> None:
        """ UnderfolderWriter for an input SamplesSequence

        :param folder: destiantion folder
        :type folder: str
        :param root_files_keys: list of keys to write as underfolder root files (only the first element
        encountered will be stored as root file, the others will be discarded), defaults to None
        :type root_files_keys: list, optional
        :param extensions_map: dictionary of regex/extension to retrieve extension for each key matching
        the corresponding regex. Unmatched keys will be stored as PICKLE object file, defaults to None
        :type extensions_map: dict, optional
        :param zfill: Length of zero padding in case of integer sample indices, defaults to 5
        :type zfill: int, optional
        :param copy_files: TRUE to copy FileSystemSample directly if not cached before, defaults to True
        :type copy_files: bool, optional
        :param use_symlinks: if TRUE (and copy_files == TRUE) the copy will be replaced with a symlink, defaults to False
        :type use_symlinks: bool, optional
        """
        self._folder = Path(folder)

        self._empty_template = root_files_keys is None and extensions_map is None
        self._root_files_keys = root_files_keys if root_files_keys is not None else []
        self._extensions_map = extensions_map if extensions_map is not None else {}
        self._zfill = zfill
        self._copy_files = copy_files
        self._use_symlinks = use_symlinks
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

        saved_root_keys = set()
        for sample in track(x):
            basename = self._build_sample_basename(sample)

            for key in sample.keys():
                if self._is_root_key(key):
                    if key not in saved_root_keys:
                        saved_root_keys.add(key)
                        itemname = f'{key}.{self._build_item_extension(key)}'
                        output_file = Path(self._folder) / itemname
                        self._write_sample_item(output_file, sample, key)
                else:
                    itemname = f'{basename}_{key}.{self._build_item_extension(key)}'
                    output_file = Path(self._datafolder) / itemname

                    self._write_sample_item(output_file, sample, key)

    def _write_sample_item(self, output_file: Path, sample: Sample, key: str):

        if self._copy_files:
            if isinstance(sample, FileSystemSample):
                item = sample.metaitem(key)
                if not sample.is_cached(key) and isinstance(item, FilesystemItem):
                    path = item.source()
                    if path.suffix == output_file.suffix:
                        if not self._use_symlinks:
                            shutil.copy(path, output_file)
                        else:
                            os.symlink(path, output_file)
                        return

        # Default action
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
