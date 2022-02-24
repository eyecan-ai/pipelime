from typing import Callable, Optional
from pathlib import Path
from pipelime.converters.base import UnderfolderConverter
from pipelime.sequences.operations import OperationResetIndices
from pipelime.sequences.readers.base import ReaderTemplate
from pipelime.sequences.samples import FileSystemSample, PlainSample, SamplesSequence
from pipelime.sequences.writers.filesystem import UnderfolderWriterV2


class SmartConverter(UnderfolderConverter):
    CONVERTED_METADATA_KEY = "conversion_metadata"

    def __init__(
        self,
        folder: str,
        extensions_map: dict,
        root_files_map: Optional[dict] = None,
        use_symlinks: bool = False,
        num_workers: int = 0,
        progress_callback: Optional[Callable[[dict], None]] = None,
    ) -> None:
        """Converts a subfolder tree structure, containing images (and metadata), to a single Underfolder.
        Subfolder structure should be like

        root
        - subfolder1
            - subfolder2
                - subfolder3
                    - image1.png
                    - image1.txt
                    - image2.png
                    - image2.txt
                - image3.png
            - image4.png

        Category for image2 will be 'subfolder1_subfolder2_subfolder3'. An so on...

        :param folder: root folder
        :type folder: str
        :param images_extension: image extension to include in conversion, defaults to "png"
        :type images_extension: str, optional
        :param use_symlinks: use symlinks instead of copying files, defaults to False
        :type use_symlinks: bool, optional
        :param num_workers: number of workers to use, defaults to 0
        :type num_workers: int, optional
        :param progress_callback: callback to report progress, defaults to None
        :type progress_callback: Optional[Callable[[dict], None]], optional
        """
        self._folder = folder
        self._use_symlinks = use_symlinks
        self._num_workers = num_workers
        self._progress_callback = progress_callback
        self._extensions_map = extensions_map
        self._root_files_map = root_files_map or {}
        self._items_map = {v: k for k, v in self._extensions_map.items()}
        self._root_items_map = {v: k for k, v in self._root_files_map.items()}

        ic = self.extract_items_and_classmap(self._folder)

        merged = self._extract_samples_map(ic["items"])

        samples = []

        for key, m in merged.items():
            extensions = list(m.keys())
            root_file = not all([x in self._items_map for x in extensions])
            if root_file:
                pass  # TODO: auto manage root files? is total madness!!
            else:
                sample = FileSystemSample(data_map={})
                for ex in extensions:
                    item_name = self._items_map[ex]
                    sample.filesmap[item_name] = m[ex]["filepath"]

                sample[SmartConverter.CONVERTED_METADATA_KEY] = m[ex]
                del sample[SmartConverter.CONVERTED_METADATA_KEY]["filepath"]

                samples.append(sample)

        self._samples = samples

    def extensions_map(self) -> dict:
        extensions_map = dict(**(self._extensions_map))
        extensions_map.update({SmartConverter.CONVERTED_METADATA_KEY: "yml"})
        return extensions_map

    def root_files_keys(self) -> dict:
        return []

    def convert(self, output_folder: str):
        """

        :param output_folder: [description]
        :type output_folder: str
        """

        output_folder = Path(output_folder)
        if not output_folder.exists():
            output_folder.mkdir(parents=True, exist_ok=False)

        writer = UnderfolderWriterV2(
            folder=output_folder,
            file_handling=UnderfolderWriterV2.FileHandling.COPY_IF_NOT_CACHED,
            copy_mode=UnderfolderWriterV2.CopyMode.HARD_LINK,
            reader_template=ReaderTemplate(
                extensions_map=self.extensions_map(),
                root_files_keys=self.root_files_keys(),
            ),
            num_workers=self._num_workers,
            progress_callback=self._progress_callback,
        )

        sequence = SamplesSequence(samples=self._samples)
        sequence = OperationResetIndices()(sequence)
        writer(sequence)
