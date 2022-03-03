import functools
import multiprocessing
import re
import shutil
from pathlib import Path
from typing import Callable, Dict, Sequence, Optional, Any
from schema import Or
from enum import Enum
from loguru import logger
from pipelime.filesystem.toolkit import FSToolkit
from pipelime.sequences.readers.base import BaseReader, ReaderTemplate
from pipelime.sequences.samples import (
    FileSystemItem,
    FileSystemSample,
    Sample,
    SamplesSequence,
)
from pipelime.sequences.writers.base import BaseWriter
from pipelime.tools.progress import pipelime_track


class UnderfolderWriter(BaseWriter):
    DATA_SUBFOLDER = "data"
    DEFAULT_EXTENSION = "pkl"
    WORKER_ID = 0

    def __init__(
        self,
        folder: str,
        root_files_keys: Optional[Sequence[str]] = None,
        extensions_map: Optional[Dict[str, str]] = None,
        zfill: int = 5,
        copy_files: bool = True,
        use_symlinks: bool = False,
        force_copy_keys: Optional[Sequence[str]] = None,
        remove_duplicates: bool = False,
        num_workers: int = 0,  # typing is here because 'schema.Optional' conflicts
        progress_callback: Optional[Callable[[dict], None]] = None,
        flush_on_write: bool = False,
    ) -> None:
        """UnderfolderWriter for an input SamplesSequence

        :param folder: destiantion folder
        :type folder: str
        :param root_files_keys: list of keys to write as underfolder root files (only
        the first element encountered will be stored as root file, the others will be
        discarded), defaults to None
        :type root_files_keys: Optional[Sequence[str]]
        :param extensions_map: dictionary of regex/extension to retrieve extension for
        each key matching the corresponding regex. Unmatched keys will be stored as
        PICKLE object file, defaults to None
        :type extensions_map: Optional[Dict[str, str]]
        :param zfill: Length of zero padding in case of integer sample indices,
        defaults to 5
        :type zfill: int, optional
        :param copy_files: TRUE to copy FileSystemSample directly if not cached before,
        defaults to True
        :type copy_files: bool, optional
        :param use_symlinks: if TRUE (and copy_files == TRUE) the copy will be replaced
        with a symlink, defaults to False
        :type use_symlinks: bool, optional
        :param force_copy_keys: A list of string keys that will be copied/symlinked
        regardless of their cached/modified state (all changes will be lost), if they
        contain file system items. This is useful in case you want to quickly write an
        item that you know for certain it was not modified, even if it was cached.
        :type force_copy_keys: Optional[Sequence[str]]
        :param remove_duplicates: if TRUE, will check for duplicates (same item with
        different extension) in the destination folder and delete them before storing,
        defaults to False
        :type remove_duplicates: bool, optional
        :param num_workers: if 0 disable multiprocessing, if -1 use Multiprocessing
        pool with all available processors, if > 0 use Multiprocessing using as many
        processes
        :type num_workers: int, optional
        :param progress_callback: callback function to call with progress information,
        defaults to None
        :type progress_callback: Optional[Callable[[dict], None]], optional
        :param flush_on_write: True to flush every sample after writing to filesystem,
        defaults to False
        :type flush_on_write: bool
        """
        self._folder = Path(folder)

        self._root_files_keys = root_files_keys if root_files_keys is not None else []
        self._extensions_map = extensions_map if extensions_map is not None else {}
        self._zfill = zfill
        self._copy_files = copy_files
        self._use_symlinks = use_symlinks
        self._force_copy_keys = force_copy_keys if force_copy_keys is not None else []
        self._remove_duplicates = remove_duplicates
        self._num_workers = num_workers
        self._progress_callback = progress_callback
        self._track = (
            pipelime_track
            if progress_callback is None
            else functools.partial(pipelime_track, disable=True)
        )
        self._datafolder = self._folder / self.DATA_SUBFOLDER
        self._datafolder.mkdir(parents=True, exist_ok=True)

        # multiprocessing attrs
        self._saved_root_keys = {}
        self._flush_on_write = flush_on_write

        if self._use_symlinks:
            import platform

            if platform.system() == "Windows":
                logger.warning(
                    "Symlink is not supported on Windows,"
                    " switching to file deep copy"
                )
                self._use_symlinks = False

    @property
    def _empty_template(self) -> bool:
        return not (self._root_files_keys or self._extensions_map)

    def _build_sample_basename(self, sample: Sample):
        if isinstance(sample.id, int):
            return str(sample.id).zfill(self._zfill)
        else:
            return str(sample.id)

    def _build_item_extension(self, key: str):
        extension = UnderfolderWriter.DEFAULT_EXTENSION
        if key in self._extensions_map:
            extension = self._extensions_map[key]
        return extension

    def _is_root_key(self, key: str):
        for r in self._root_files_keys:
            if re.match(r, key) is not None:
                return True
        return False

    def _process_sample(self, sample: Sample, only_root_keys: bool = False):
        basename = self._build_sample_basename(sample)

        for key in sample.keys():
            if self._is_root_key(key):
                # Write root keys only once
                if key not in self._saved_root_keys:
                    self._saved_root_keys[key] = True
                    itemname = f"{key}.{self._build_item_extension(key)}"
                    output_file = Path(self._folder) / itemname
                    self._write_sample_item(output_file, sample, key)
            elif not only_root_keys:
                itemname = f"{basename}_{key}.{self._build_item_extension(key)}"
                output_file = Path(self._datafolder) / itemname

                self._write_sample_item(output_file, sample, key)

        if self._flush_on_write:
            sample.flush()

    def __call__(self, x: SamplesSequence) -> None:
        if len(x) == 0:
            return

        if isinstance(x, BaseReader) and self._empty_template:
            template = x.get_reader_template()
            if template is not None:
                self._extensions_map = template.extensions_map
                self._root_files_keys = list(template.root_files_keys)
                self._zfill = template.idx_length

        self._zfill = max(self._zfill, x.best_zfill())

        self._saved_root_keys = {}
        if self._num_workers > 1 or self._num_workers == -1:
            # first save root keys, if any
            self._process_sample(x[0], True)

            with multiprocessing.Pool(
                processes=None if self._num_workers == -1 else self._num_workers
            ) as pool:
                list(
                    self._track(
                        pool.imap_unordered(self._process_sample, x),
                        total=len(x),
                        description="Writing Underfolder",
                        track_callback=self._progress_callback,
                    )
                )
        else:
            for sample in self._track(
                x,
                description="Writing Underfolder",
                track_callback=self._progress_callback,
            ):
                self._process_sample(sample)

    def _copy_filesystem_item(self, output_file: Path, item: FileSystemItem) -> None:
        path = item.source()
        if path != output_file:
            if not self._use_symlinks:
                shutil.copy(path, output_file)
            else:
                output_file.symlink_to(path)

    def _dump_cached_item(self, output_file: Path, item: Any):
        FSToolkit.store_data(str(output_file), item)

    def _remove_duplicate_files(self, output_file: Path):
        duplicates = list(output_file.parent.glob(f"{output_file.stem}.*"))
        for f in duplicates:
            if f.suffix != output_file.suffix:
                f.unlink()

    def _write_sample_item(self, output_file: Path, sample: Sample, key: str):
        copy_item = False

        if self._remove_duplicates:
            self._remove_duplicate_files(output_file)

        if self._copy_files and isinstance(sample, FileSystemSample):
            item = sample.metaitem(key)
            if (
                not sample.is_cached(key) or key in self._force_copy_keys
            ) and isinstance(item, FileSystemItem):
                path = item.source()
                if path.suffix == output_file.suffix:
                    copy_item = True

        if copy_item:
            self._copy_filesystem_item(output_file, item)
        else:
            self._dump_cached_item(output_file, sample[key])

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


class UnderfolderWriterV2(UnderfolderWriter):
    class FileHandling(Enum):
        ALWAYS_WRITE_FROM_CACHE = 0
        ALWAYS_COPY_FROM_DISK = 1
        COPY_IF_NOT_CACHED = 2

    class CopyMode(Enum):
        DEEP_COPY = "deepcopy"
        SYM_LINK = "symlink"
        HARD_LINK = "hardlink"

    def __init__(
        self,
        folder: str,
        file_handling: FileHandling = FileHandling.COPY_IF_NOT_CACHED,
        copy_mode: CopyMode = CopyMode.DEEP_COPY,
        force_copy_keys: Optional[Sequence[str]] = None,
        reader_template: Optional[ReaderTemplate] = None,
        remove_duplicates: bool = False,
        num_workers: int = 0,
        progress_callback: Optional[Callable[[dict], None]] = None,
        flush_on_write: bool = False,
    ):
        root_file_keys, extensions_map, zfill = (
            (
                reader_template.root_files_keys,
                reader_template.extensions_map,
                reader_template.idx_length,
            )
            if reader_template is not None
            else (None, None, 5)
        )

        super().__init__(
            folder=folder,
            root_files_keys=root_file_keys,
            extensions_map=extensions_map,
            zfill=zfill,
            force_copy_keys=force_copy_keys,
            remove_duplicates=remove_duplicates,
            num_workers=num_workers,
            progress_callback=progress_callback,
            flush_on_write=flush_on_write,
        )

        self._file_handling = file_handling
        self._copy_mode = copy_mode
        self._flush_on_write = flush_on_write

        if self._copy_mode is UnderfolderWriterV2.CopyMode.SYM_LINK:
            import platform

            if platform.system() == "Windows":
                logger.warning(
                    "Symlink is not supported on Windows,"
                    " switching to file deep copy"
                )
                self._copy_mode = UnderfolderWriterV2.CopyMode.DEEP_COPY

    def _copy_filesystem_item(self, output_file: Path, item: FileSystemItem) -> None:
        path = item.source()
        if path != output_file:
            if self._copy_mode is UnderfolderWriterV2.CopyMode.DEEP_COPY:
                shutil.copy(path, output_file)
            elif self._copy_mode is UnderfolderWriterV2.CopyMode.SYM_LINK:
                output_file.symlink_to(path)
            elif self._copy_mode is UnderfolderWriterV2.CopyMode.HARD_LINK:

                # TODO: if you call the writer __call__ multiple times, due to batching,
                # and the root_files_keys are the same, the hard link will throw an error
                # because the file already exists. This is a workaround for now.
                if not output_file.exists():
                    try:
                        # (new in version 3.10)
                        output_file.hardlink_to(path)  # type: ignore
                    except AttributeError:
                        import os

                    os.link(path, output_file)

    def _write_sample_item(self, output_file: Path, sample: Sample, key: str):
        if self._remove_duplicates:
            self._remove_duplicate_files(output_file)

        if (
            self._file_handling
            is not UnderfolderWriterV2.FileHandling.ALWAYS_WRITE_FROM_CACHE
        ):
            # copy source file if possible
            def _is_item_cached():
                try:
                    # sample is, eg, a FileSystemSample
                    return sample.is_cached(key)  # type: ignore
                except AttributeError:  # pragma: no cover
                    return False  # pragma: no cover

            meta_item = sample.metaitem(key)
            if (
                isinstance(meta_item, FileSystemItem)
                and meta_item.source().suffix == output_file.suffix
                and (
                    self._file_handling
                    is UnderfolderWriterV2.FileHandling.ALWAYS_COPY_FROM_DISK
                    or key in self._force_copy_keys
                    or not _is_item_cached()
                )
            ):
                # * it is a file AND
                # * suffix does not change (eg, no image re-encoding) AND
                # ** always copy OR
                # ** this key must be always copied OR
                # ** item is not cached
                self._copy_filesystem_item(output_file, meta_item)
                return

        # if any of the above checks failed, just dump the item value
        self._dump_cached_item(output_file, sample[key])
