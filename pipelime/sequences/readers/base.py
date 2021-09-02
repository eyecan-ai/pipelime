from typing import Dict, Sequence, Union
from pipelime.factories import Bean
from pipelime.sequences.samples import SamplesSequence
from deepdiff import DeepDiff


class ReaderTemplate(object):

    def __init__(
        self,
        extensions_map: Dict[str, any],
        root_files_keys: Sequence[str],
        idx_length: int = 5
    ):
        self.extensions_map = extensions_map
        self.root_files_keys = root_files_keys
        self.idx_length = idx_length

    def __eq__(self, o: object) -> bool:
        equality = True
        equality = equality and (not DeepDiff(o.extensions_map, self.extensions_map))
        equality = equality and (set(self.root_files_keys) == set(o.root_files_keys))
        equality = equality and (self.idx_length == o.idx_length)
        return equality


class BaseReader(SamplesSequence, Bean):

    def get_reader_template(self) -> Union[ReaderTemplate, None]:
        """ Retrieves the template of the reader, i.e. a mapping
        between sample_key/file_extension/encoding and a list of root files keys

        :rtype: Union[ReaderTemplate, None]
        """
