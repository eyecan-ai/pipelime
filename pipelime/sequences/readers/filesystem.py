import multiprocessing
from pathlib import Path
from typing import Union

from loguru import logger
from schema import Optional

from pipelime.filesystem.toolkit import FSToolkit
from pipelime.sequences.readers.base import BaseReader, ReaderTemplate
from pipelime.sequences.samples import FileSystemSample
import functools


class UnderfolderReader(BaseReader):
    DATA_SUBFOLDER = "data"
    PRIVATE_KEY_QUALIFIER = "_"
    PRIVATE_KEY_UNDERFOLDER_LINKS = "underfolder_links"

    def __init__(
        self,
        folder: str,
        copy_root_files: bool = True,
        lazy_samples: bool = True,
        num_workers: int = 0,
    ) -> None:

        self._folder = Path(folder).resolve()
        self._copy_root_files = copy_root_files
        self._datafolder = self._folder / self.DATA_SUBFOLDER
        self._lazy_samples = lazy_samples
        self._num_workers = num_workers
        assert self._datafolder.exists(), f'No data folder found: "{self._datafolder}"'

        # builds tree from subfolder with underscore notation
        self._tree = FSToolkit.tree_from_underscore_notation_files(self._datafolder)
        self._ids = list(sorted(self._tree.keys()))

        # extract all root files
        self._root_files = [x for x in Path(self._folder).glob("*") if x.is_file()]

        # purge hidden files from root files
        self._root_files = [x for x in self._root_files if not x.name.startswith(".")]

        # extract private root files among root files
        self._private_root_files = list(
            filter(
                lambda x: x.name.startswith(self.PRIVATE_KEY_QUALIFIER),
                self._root_files,
            )
        )

        # purge private root files from root files
        self._root_files = list(
            filter(
                lambda x: not x.name.startswith(self.PRIVATE_KEY_QUALIFIER),
                self._root_files,
            )
        )

        # build public root data
        self._root_data = {}
        self._root_files_keys = set()
        for f in self._root_files:
            self._root_files_keys.add(f.stem)
            self._root_data[f.stem] = str(f)

        # build private root data
        self._root_private_files_keys = set()
        self._root_private_data = {}
        for f in self._private_root_files:
            self._root_private_files_keys.add(
                f.stem.replace(self.PRIVATE_KEY_QUALIFIER, "", 1)
            )
            self._root_private_data[
                f.stem.replace(self.PRIVATE_KEY_QUALIFIER, "", 1)
            ] = str(
                f
            )  # FSToolkit.load_data(f)

        # Load samples
        if self._num_workers == -1 or self._num_workers > 0:
            if self._lazy_samples:
                logger.warning(f"Multiprocessing with Lazy Samples are useless!")
            pool = multiprocessing.Pool(
                None if self._num_workers == -1 else self._num_workers
            )
            samples = list(pool.imap(self._read_sample, range(len(self._ids))))
        else:
            samples = []
            for idx in range(len(self._ids)):
                samples.append(self._read_sample(idx))

        # Manage private keys
        if self.PRIVATE_KEY_UNDERFOLDER_LINKS in self._root_private_data:
            underfolder_links = FSToolkit.load_data(
                self._root_private_data[self.PRIVATE_KEY_UNDERFOLDER_LINKS]
            )
            for link in underfolder_links:
                if Path(link).exists():
                    linked_reader = UnderfolderReader(
                        folder=link,
                        copy_root_files=copy_root_files,
                        lazy_samples=lazy_samples,
                        num_workers=num_workers,
                    )
                    if len(linked_reader) != len(samples):
                        raise ValueError(
                            f"Linked reader has a different number of samples ({len(linked_reader)}) than the current reader ({len(samples)})"
                        )

                    self._root_data.update(linked_reader._root_data)
                    self._root_files_keys.update(linked_reader._root_files_keys)

                    samples = [x.merge(y) for x, y in zip(linked_reader, samples)]

        super().__init__(samples=samples)

    def _read_sample(self, idx: int):
        data = dict(self._tree[self._ids[idx]])
        if self._copy_root_files:
            data.update(self._root_data)

        return FileSystemSample(
            data_map=data, lazy=self._lazy_samples, id=self._ids[idx]
        )

    def is_root_key(self, key: str):
        return key in self._root_files_keys

    def is_root_private_key(self, key: str):
        return key in self._root_private_files_keys

    def get_reader_template(self) -> Union[ReaderTemplate, None]:
        """Retrieves the template of the underfolder reader, i.e. a mapping
        between sample_key/file_extension and a list of root files keys

        :raises TypeError: If first sample is not a FileSystemSample
        :return: None if dataset is empty, otherwise an ReaderTemplate
        :rtype: Union[ReaderTemplate, None]
        """

        if len(self) > 0:
            sample = self[0]
            if not isinstance(sample, FileSystemSample):
                raise TypeError(f"Anomalous sample type found: {type(sample)}")

            extensions_map = {}
            idx_length = len(str(sample.id))
            for key, filename in sample.filesmap.items():
                extensions_map[key] = Path(filename).suffix.replace(".", "")

            return ReaderTemplate(
                extensions_map=extensions_map,
                root_files_keys=list(self._root_files_keys),
                idx_length=idx_length,
            )
        else:
            None

    @classmethod
    def get_reader_template_from_folder(
        cls, folder: str
    ) -> Union[ReaderTemplate, None]:
        """Helper class function to retrieve a reader template directly from folder

        :param folder: underfolder folder
        :type folder: str
        :return: ReaderTemplate of the loaded underfolder
        :rtype: Union[ReaderTemplate, None]
        """
        reader = UnderfolderReader(
            folder=folder, copy_root_files=True, lazy_samples=True
        )
        return reader.get_reader_template()

    def flush(self):
        """Clear cache for each internal FileSystemSample"""
        for sample in self:
            sample.flush()

    @classmethod
    def bean_schema(cls) -> dict:
        return {"folder": str, Optional("copy_root_files"): bool}

    @classmethod
    def from_dict(cls, d: dict):
        return UnderfolderReader(
            folder=d.get("folder"), copy_root_files=d.get("copy_root_files", True)
        )

    def to_dict(self) -> dict:
        return {"folder": str(self._folder), "copy_root_files": self._copy_root_files}

    @classmethod
    def link_underfolders(cls, source_folder: str, target_folder: str):
        """Links two Underfolder adding target_folder to links in source_folder

        :param source_folder: Underfolder folder where to add links
        :type source_folder: str
        :param target_folder: Underfolder folder to add
        :type target_folder: str
        """

        source_folder = Path(source_folder)
        source_reader = UnderfolderReader(folder=source_folder, lazy_samples=True)

        # Builds private key filename
        key = cls.PRIVATE_KEY_UNDERFOLDER_LINKS
        private_key_file = source_folder / f"{cls.PRIVATE_KEY_QUALIFIER}{key}.yml"

        # Create private key file if not present
        if not source_reader.is_root_private_key(key):
            FSToolkit.store_data(private_key_file, [])

        # Loads private key data
        prev_links = FSToolkit.load_data(private_key_file)

        # Update private key data
        prev_links.append(target_folder)
        FSToolkit.store_data(private_key_file, prev_links)
