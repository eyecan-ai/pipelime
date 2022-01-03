from pathlib import Path
import h5py
from rich.progress import track
from schema import Optional, Or
from pipelime.h5.toolkit import H5Database, H5ToolKit
from pipelime.sequences.readers.base import BaseReader
from pipelime.sequences.samples import Sample, SamplesSequence
from pipelime.sequences.writers.base import BaseWriter


class H5Writer(BaseWriter):
    DATA_SUBFOLDER = "data"
    DEFAULT_EXTENSION = None

    def __init__(
        self,
        filename: str,
        root_files_keys: list = None,
        extensions_map: dict = None,
        zfill: int = 5,
    ) -> None:
        """H5Writer for an input SamplesSequence

        :param filename: destiantion hdf5 filename
        :type filename: str
        :param root_files_keys: list of keys to write as underfolder root files (only the first element
        encountered will be stored as root file, the others will be discarded), defaults to None
        :type root_files_keys: list, optional
        :param extensions_map: dictionary of regex/extension to retrieve extension for each key matching
        the corresponding regex. Unmatched keys will be stored as PICKLE object file, defaults to None
        :type extensions_map: dict, optional
        :param zfill: Length of zero padding in case of integer sample indices, defaults to 5
        :type zfill: int, optional
        """
        self._filename = Path(filename)

        self._empty_template = root_files_keys is None and extensions_map is None
        self._root_files_keys = root_files_keys if root_files_keys is not None else []
        self._extensions_map = extensions_map if extensions_map is not None else {}
        self._zfill = zfill
        self._h5database = H5Database(filename=self._filename, readonly=False)

    def _build_sample_basename(self, sample: Sample):
        if isinstance(sample.id, int):
            return str(sample.id).zfill(self._zfill)
        else:
            return str(sample.id)

    def _build_item_extension(self, key: str):
        return self._extensions_map.get(key, H5Writer.DEFAULT_EXTENSION)

    def _is_root_key(self, key: str):
        return key in self._root_files_keys

    def __call__(self, x: SamplesSequence) -> None:

        if isinstance(x, BaseReader) and self._empty_template:
            template = x.get_reader_template()
            if template is not None:
                self._extensions_map = template.extensions_map
                self._root_files_keys = list(template.root_files_keys)
                self._zfill = template.idx_length

        self._zfill = max(self._zfill, x.best_zfill())

        self._h5database.open()
        saved_root_keys = set()

        # Populate global links
        global_links = {}
        for sample in x:
            basename = self._build_sample_basename(sample)
            for key in sample.keys():
                if self._is_root_key(key):
                    if key not in saved_root_keys:
                        group = self._h5database.get_global_group(
                            key=basename, force_create=True
                        )
                        H5ToolKit.store_data(
                            group,
                            key=key,
                            data=sample[key],
                            encoding=self._build_item_extension(key),
                        )
                        # h5py.SoftLink(self._h5database.get_global_group_name(key=basename))
                        global_links[key] = h5py.SoftLink(group[key].name)
            break

        for sample in track(x, description="Writing H5"):
            basename = self._build_sample_basename(sample)

            for key in sample.keys():
                if not self._is_root_key(key):
                    group = self._h5database.get_sample_group(
                        key=basename, force_create=True
                    )
                    H5ToolKit.store_data(
                        group,
                        key=key,
                        data=sample[key],
                        encoding=self._build_item_extension(key),
                    )
                else:
                    group = self._h5database.get_sample_group(
                        key=basename, force_create=True
                    )
                    group[key] = global_links[key]

        self._h5database.close()

    @classmethod
    def bean_schema(cls) -> dict:
        return {
            "filename": str,
            Optional("root_files_keys"): Or(None, list),
            Optional("extensions_map"): Or(None, dict),
            Optional("zfill"): int,
        }

    @classmethod
    def from_dict(cls, d: dict):
        return H5Writer(
            filename=d.get("filename"),
            root_files_keys=d.get("root_files_keys", None),
            extensions_map=d.get("extensions_map", None),
            zfill=d.get("zfill", 5),
        )

    def to_dict(self) -> dict:
        return {
            "filename": str(self._filename),
            "root_files_keys": self._root_files_keys,
            "extensions_map": self._extensions_map,
            "zfill": self._zfill,
        }
