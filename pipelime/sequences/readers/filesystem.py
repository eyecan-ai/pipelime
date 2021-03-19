from pathlib import Path
from typing import Dict, Sequence, Union
from pipelime.factories import Bean, BeanFactory
from pipelime.sequences.readers.base import BaseReader
from pipelime.sequences.samples import FileSystemSample
from pipelime.filesystem.toolkit import FSToolkit
from schema import Optional


class UnderfolderReaderTemplate(object):

    def __init__(
        self,
        extensions_map: Dict[str, any],
        root_files_keys: Sequence[str],
        idx_length: int = 5
    ):
        self.extensions_map = extensions_map
        self.root_files_keys = root_files_keys
        self.idx_length = idx_length


class UnderfolderReader(BaseReader):
    DATA_SUBFOLDER = 'data'

    def __init__(self, folder: str, copy_root_files: bool = True, lazy_samples: bool = True) -> None:

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
        self._root_files_keys = set()
        for f in self._root_files:
            self._root_files_keys.add(f.stem)
            self._root_data[f.stem] = str(f)  # FSToolkit.load_data(f)

        samples = []
        for idx in range(len(self._ids)):
            data = dict(self._tree[self._ids[idx]])
            if self._copy_root_files:
                data.update(self._root_data)

            sample = FileSystemSample(data_map=data, lazy=lazy_samples, id=self._ids[idx])
            samples.append(sample)

        super().__init__(samples=samples)

    def get_filesystem_template(self) -> Union[UnderfolderReaderTemplate, None]:
        """ Retrieves the template of the underfolder reader, i.e. a mapping
        between sample_key/file_extension and a list of root files keys

        :raises TypeError: If first sample is not a FileSystemSample
        :return: None if dataset is empty, otherwise an UnderfolderReaderTemplate
        :rtype: Union[UnderfolderReaderTemplate, None]
        """

        if len(self) > 0:
            sample = self[0]
            if not isinstance(sample, FileSystemSample):
                raise TypeError(f'Anomalous sample type found: {type(sample)}')

            extensions_map = {}
            idx_length = len(str(sample.id))
            for key, filename in sample.filesmap.items():
                extensions_map[key] = Path(filename).suffix.replace('.', '')

            return UnderfolderReaderTemplate(
                extensions_map=extensions_map,
                root_files_keys=list(self._root_files_keys),
                idx_length=idx_length
            )
        else:
            None

    @classmethod
    def bean_schema(cls) -> dict:
        return {
            'folder': str,
            Optional('copy_root_files'): bool
        }

    @classmethod
    def from_dict(cls, d: dict):
        return UnderfolderReader(
            folder=d.get('folder'),
            copy_root_files=d.get('copy_root_files', True)
        )

    def to_dict(self) -> dict:
        return {
            'folder': str(self._folder),
            'copy_root_files': self._copy_root_files
        }


BeanFactory.register_bean(UnderfolderReader)
