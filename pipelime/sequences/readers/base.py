from typing import Dict, Sequence, Union

from choixe.spooks import Spook
from deepdiff import DeepDiff

from pipelime.sequences.samples import SamplesSequence


class ReaderTemplate(object):
    def __init__(
        self,
        extensions_map: Dict[str, any] = None,
        root_files_keys: Sequence[str] = None,
        idx_length: int = 5,
    ):
        self.extensions_map = extensions_map if extensions_map is not None else {}
        self.root_files_keys = root_files_keys if root_files_keys else []
        self.idx_length = idx_length

    def __eq__(self, o: "ReaderTemplate") -> bool:
        equality = True
        equality = equality and (not DeepDiff(o.extensions_map, self.extensions_map))
        equality = equality and (set(self.root_files_keys) == set(o.root_files_keys))
        equality = equality and (self.idx_length == o.idx_length)
        return equality


class BaseReader(SamplesSequence, Spook):
    def get_reader_template(self) -> Union[ReaderTemplate, None]:
        """Retrieves the template of the reader, i.e. a mapping
        between sample_key/file_extension/encoding and a list of root files keys

        :rtype: Union[ReaderTemplate, None]
        """
