import multiprocessing
import os
import re
import shutil
from pathlib import Path

from choixe.spooks import Spook
from rich.progress import track
from schema import Optional, Or

from pipelime.filesystem.toolkit import FSToolkit
from pipelime.sequences.readers.base import BaseReader
from pipelime.sequences.samples import (
    FilesystemItem,
    FileSystemSample,
    Sample,
    SamplesSequence,
)
from pipelime.sequences.writers.base import BaseWriter


class UnderfolderWriter(BaseWriter):
    DATA_SUBFOLDER = "data"
    DEFAULT_EXTENSION = "pkl"

    def __init__(
        self,
        folder: str,
        root_files_keys: list = None,
        extensions_map: dict = None,
        zfill: int = 5,
        copy_files: bool = True,
        use_symlinks: bool = False,
        force_copy_keys: list = None,
        num_workers: int = 0,  # typing is here because 'schema.Optional' conflicts
    ) -> None:
        """UnderfolderWriter for an input SamplesSequence

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
        :param force_copy_keys: A list of string keys that will be copied/symlinked regardless of their
        cached/modified state (all changes will be lost), if they contain file system items.
        This is useful in case you want to quickly write an item that you know for certain it was not modified, even if it was cached.
        :type force_copy_keys: list, optional
        :param num_workers: if 0 disable multiprocessing, if -1 use Multiprocessing pool with all available processors, if > 0 use Multiprocessing using as many processes
        :type num_workers: int, optional
        """
        self._folder = Path(folder)

        self._empty_template = root_files_keys is None and extensions_map is None
        self._root_files_keys = root_files_keys if root_files_keys is not None else []
        self._extensions_map = extensions_map if extensions_map is not None else {}
        self._zfill = zfill
        self._copy_files = copy_files
        self._use_symlinks = use_symlinks
        self._num_workers = num_workers

        if force_copy_keys is None:
            force_copy_keys = []

        self._force_copy_keys = force_copy_keys
        self._datafolder = self._folder / self.DATA_SUBFOLDER
        if not self._datafolder.exists():
            self._datafolder.mkdir(parents=True, exist_ok=True)

        # multiprocessing attrs
        self._saved_root_keys = None

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

    def _process_sample(self, sample: Sample):
        basename = self._build_sample_basename(sample)

        for key in sample.keys():
            if self._is_root_key(key):
                # Write root keys only once
                if key not in self._saved_root_keys:
                    self._saved_root_keys[key] = True
                    itemname = f"{key}.{self._build_item_extension(key)}"
                    output_file = Path(self._folder) / itemname
                    self._write_sample_item(output_file, sample, key)
            else:
                itemname = f"{basename}_{key}.{self._build_item_extension(key)}"
                output_file = Path(self._datafolder) / itemname

                self._write_sample_item(output_file, sample, key)

    def __call__(self, x: SamplesSequence) -> None:

        if isinstance(x, BaseReader) and self._empty_template:
            template = x.get_reader_template()
            if template is not None:
                self._extensions_map = template.extensions_map
                self._root_files_keys = list(template.root_files_keys)
                self._zfill = template.idx_length

        self._zfill = max(self._zfill, x.best_zfill())

        if self._num_workers > 0 or self._num_workers == -1:
            manager = multiprocessing.Manager()
            self._saved_root_keys = manager.dict()
            pool = multiprocessing.Pool(
                processes=None if self._num_workers == -1 else self._num_workers
            )
            list(track(pool.imap_unordered(self._process_sample, x), total=len(x)))
        else:
            self._saved_root_keys = {}
            for sample in track(x):
                self._process_sample(sample)

    def _copy_filesystem_item(self, output_file: Path, item: FilesystemItem) -> None:
        path = item.source()
        if not self._use_symlinks:
            if path != output_file:
                shutil.copy(path, output_file)
        else:
            os.symlink(path, output_file)

    def _write_sample_item(self, output_file: Path, sample: Sample, key: str):
        copy_item = False
        if self._copy_files and isinstance(sample, FileSystemSample):
            item = sample.metaitem(key)
            if (
                not sample.is_cached(key) or key in self._force_copy_keys
            ) and isinstance(item, FilesystemItem):
                path = item.source()
                if path.suffix == output_file.suffix:
                    copy_item = True

        if copy_item:
            self._copy_filesystem_item(output_file, item)
        else:
            item = sample[key]
            FSToolkit.store_data(output_file, item)

    @classmethod
    def bean_schema(cls) -> dict:
        return {
            "folder": str,
            Optional("root_files_keys"): Or(None, list),
            Optional("extensions_map"): Or(None, dict),
            Optional("zfill"): int,
        }

    @classmethod
    def from_dict(cls, d: dict):
        return UnderfolderWriter(
            folder=d.get("folder"),
            root_files_keys=d.get("root_files_keys", None),
            extensions_map=d.get("extensions_map", None),
            zfill=d.get("zfill", 5),
        )

    def to_dict(self) -> dict:
        return {
            "folder": str(self._folder),
            "root_files_keys": self._root_files_keys,
            "extensions_map": self._extensions_map,
            "zfill": self._zfill,
        }
