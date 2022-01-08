import multiprocessing
from pathlib import Path
from typing import Dict, Union

from loguru import logger
import networkx as nx
from typing import Optional
from pipelime.filesystem.toolkit import FSToolkit
from pipelime.sequences.readers.base import BaseReader, ReaderTemplate
from pipelime.sequences.samples import FileSystemSample
from pipelime.sequences.stages import SampleStage


class UnderfolderReader(BaseReader):
    DATA_SUBFOLDER = "data"
    PRIVATE_KEY_QUALIFIER = "_"

    def __init__(
        self,
        folder: str,
        copy_root_files: bool = True,
        lazy_samples: bool = True,
        num_workers: int = 0,
        enable_plugins: bool = True,
    ) -> None:
        """Initialize a new underfolder reader

        :param folder: Path to the root folder
        :type folder: str
        :param copy_root_files: TRUE to propagate root files to samples, defaults to True
        :type copy_root_files: bool, optional
        :param lazy_samples: TRUE to load only files skeleton and not data within , defaults to True
        :type lazy_samples: bool, optional
        :param num_workers: num workers for multiprocessing data loading , defaults to 0
        :type num_workers: int, optional
        :param enable_plugins:  TRUE to enable plugins activation, defaults to True
        :type enable_plugins: bool, optional
        :raises FileNotFoundError: [description]
        """

        self._folder = Path(folder).resolve()
        self._copy_root_files = copy_root_files
        self._datafolder = self._folder / self.DATA_SUBFOLDER
        self._lazy_samples = lazy_samples
        self._num_workers = num_workers

        # Checks for valid folder
        if not self._datafolder.exists():
            raise FileNotFoundError(
                f"It seems not a valid Underfolder, folder {self._datafolder} not found"
            )

        # builds tree from subfolder with underscore notation
        self._tree = FSToolkit.tree_from_underscore_notation_files(self._datafolder)
        self._ids = list(sorted(self._tree.keys()))

        # extract all root files
        root_files = [x for x in Path(self._folder).glob("*") if x.is_file()]

        # purge hidden files from root files
        root_files = [x for x in root_files if not x.name.startswith(".")]

        # extract private root files among root files
        private_root_files = list(
            filter(
                lambda x: x.name.startswith(self.PRIVATE_KEY_QUALIFIER),
                root_files,
            )
        )

        # purge private root files from root files
        root_files = list(
            filter(
                lambda x: not x.name.startswith(self.PRIVATE_KEY_QUALIFIER),
                root_files,
            )
        )

        # build public root data
        self._root_data = {}
        self._root_files_keys = set()
        for f in root_files:
            self._root_files_keys.add(f.stem)
            self._root_data[f.stem] = str(f)

        # build private root data
        self._root_private_files_keys = set()
        self._root_private_data = {}
        for f in private_root_files:
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
                logger.warning("Multiprocessing with Lazy Samples are useless!")
            pool = multiprocessing.Pool(
                None if self._num_workers == -1 else self._num_workers
            )
            samples = list(pool.imap(self._read_sample, range(len(self._ids))))
        else:
            samples = []
            for idx in range(len(self._ids)):
                samples.append(self._read_sample(idx))

        super().__init__(samples=samples)

        # Spawn plugins
        self._plugins_map = UnderfolderPlugins.parse(self) if enable_plugins else {}

    def _read_sample(self, idx: int):
        data = dict(self._tree[self._ids[idx]])
        if self._copy_root_files:
            data.update(self._root_data)

        purged_id = self.purge_id(self._ids[idx])
        return FileSystemSample(data_map=data, lazy=self._lazy_samples, id=purged_id)

    @property
    def plugins_map(self) -> Dict[str, "UnderfolderPlugin"]:
        return self._plugins_map

    @property
    def folder(self) -> Path:
        return self._folder

    @property
    def lazy_samples(self) -> bool:
        return self._lazy_samples

    @property
    def copy_root_files(self) -> bool:
        return self._copy_root_files

    @property
    def num_workers(self) -> int:
        return self._num_workers

    @property
    def root_data(self) -> dict:
        return self._root_data

    @property
    def root_files_keys(self) -> dict:
        return self._root_files_keys

    @property
    def root_private_data(self):
        return self._root_private_data

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
            idx_length = len(self._ids[0])
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


class UnderfolderPlugin:
    PLUGIN_NAME = ""

    def apply(self, reader: UnderfolderReader, plugin_data: any):
        """Generic method to apply a plugin to a reader

        :param reader: UnderfolderReader to apply the plugin to
        :type reader: UnderfolderReader
        :param plugin_data: Plugin data, can be any data loadable by FSToolkit
        :type plugin_data: any
        """
        pass


class UnderfolderLinksPlugin(UnderfolderPlugin):
    PLUGIN_NAME = "underfolder_links"

    def __init__(self) -> None:
        super().__init__()
        self._links_graph = None

    @property
    def links_graph(self) -> nx.Graph:
        return self._links_graph

    def apply(self, reader: UnderfolderReader, plugin_data: any):
        self._links_graph = self.build_links_graph(reader.folder)
        underfolder_links: dict = plugin_data
        for link in underfolder_links:
            if Path(link).exists():
                linked_reader = UnderfolderReader(
                    folder=link,
                    copy_root_files=reader.copy_root_files,
                    lazy_samples=reader.lazy_samples,
                    num_workers=reader.num_workers,
                )
                if len(linked_reader) != len(reader.samples):
                    raise ValueError(
                        f"Linked reader has a different number of samples"
                        f"({len(linked_reader)}) than the current reader ({len(reader.samples)})"
                    )

                reader.root_data.update(linked_reader.root_data)
                reader.root_files_keys.update(linked_reader.root_files_keys)
                reader.samples = [
                    x.merge(y) for x, y in zip(linked_reader, reader.samples)
                ]

    @classmethod
    def build_links_graph(cls, source_folder: str, graph: Optional[nx.DiGraph] = None):
        source_reader = UnderfolderReader(
            folder=source_folder, lazy_samples=True, enable_plugins=False
        )
        if graph is None:
            graph = nx.DiGraph()

        if cls.PLUGIN_NAME in source_reader.root_private_data:
            links = FSToolkit.load_data(
                source_reader.root_private_data[cls.PLUGIN_NAME]
            )
            for link in links:
                graph.add_edge(source_folder, link)
                cls.build_links_graph(link, graph)

        loops = []
        try:
            loops = nx.find_cycle(graph)
        except Exception:
            pass
        if len(loops) > 0:
            raise RuntimeError("Cycle detected in the Underfolder links graph")
        return graph

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
        target_reader = UnderfolderReader(folder=target_folder, lazy_samples=True)
        if len(source_reader) != len(target_reader):
            raise RuntimeError(
                "Cannot link underfolders with different number of samples"
            )

        # Builds private key filename
        key = UnderfolderLinksPlugin.PLUGIN_NAME
        private_key_file = (
            source_folder / f"{UnderfolderReader.PRIVATE_KEY_QUALIFIER}{key}.yml"
        )

        # Create private key file if not present
        if not source_reader.is_root_private_key(key):
            FSToolkit.store_data(private_key_file, [])

        # Loads private key data
        prev_links = FSToolkit.load_data(private_key_file)

        # Update private key data
        prev_links.append(target_folder)
        FSToolkit.store_data(private_key_file, prev_links)


class UnderfolderStagePlugin(UnderfolderPlugin):
    PLUGIN_NAME = "underfolder_stage"

    def __init__(self) -> None:
        super().__init__()

    def apply(self, reader: UnderfolderReader, plugin_data: any):
        reader.stage = SampleStage.create(plugin_data)

    @classmethod
    def set_stages(cls, source_folder: str, stage: SampleStage):
        """Sets the Stage of the entire Underfolder

        :param source_folder: Underfolder folder
        :type source_folder: str
        :param stage: Sample stage to set
        :type stage: SampleStage
        """

        source_folder = Path(source_folder)
        UnderfolderReader(folder=source_folder, lazy_samples=True)

        # Builds private key filename
        key = UnderfolderStagePlugin.PLUGIN_NAME
        private_key_file = (
            source_folder / f"{UnderfolderReader.PRIVATE_KEY_QUALIFIER}{key}.yml"
        )

        # Create private key file if not present
        FSToolkit.store_data(private_key_file, stage.serialize())


class UnderfolderPlugins:

    PLUGINS_MAP = {
        UnderfolderLinksPlugin.PLUGIN_NAME: UnderfolderLinksPlugin,
        UnderfolderStagePlugin.PLUGIN_NAME: UnderfolderStagePlugin,
    }

    @classmethod
    def parse(cls, reader: UnderfolderReader) -> Dict[str, UnderfolderPlugin]:
        plugins_map = {}
        for key in reader.root_private_data:
            if key in cls.PLUGINS_MAP:
                plugin = cls.PLUGINS_MAP[key]()
                plugin.apply(reader, FSToolkit.load_data(reader.root_private_data[key]))
                plugins_map[key] = plugin

        return plugins_map
