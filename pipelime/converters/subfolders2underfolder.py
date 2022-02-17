import os
from typing import Callable, Dict, Optional, Sequence
from pathlib import Path
from pipelime.converters.base import UnderfolderConverter
from pipelime.sequences.readers.base import ReaderTemplate
from pipelime.sequences.samples import FileSystemSample, SamplesSequence
from pipelime.sequences.writers.filesystem import UnderfolderWriterV2
import re


class Subfolders2Underfolder(UnderfolderConverter):
    CHAR_TO_REPLACE = ["/", "\\", ":", "*", "?", '"', "<", ">", "|", "-"]

    def __init__(
        self,
        folder: str,
        images_extension: str = "png",
        use_symlinks: bool = False,
        num_workers: int = 0,
        progress_callback: Optional[Callable[[dict], None]] = None,
    ) -> None:
        """Converts a subfolder tree structure, containing images, to a single Underfolder.
        Subfolder structure should be like

        root
        - subfolder1
            - subfolder2
                - subfolder3
                    - image1.png
                    - image2.png
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
        self._images_extension = images_extension
        out = self.remap(self.extract_subfolders_and_files(folder))
        self._items = out["items"]
        self._classmap = out["classmap"]

    def extract_subfolders_and_files(self, folder: str) -> Sequence[Dict]:
        """extract subfolders and files from a folder recursively

        :param folder: root folder
        :type folder: str
        :return: list of all subfolders (recursive) with associated files
        :rtype: Sequence[Dict]
        """
        subfolders = []
        for root, dirs, files in os.walk(folder):
            for dir in dirs:
                subfolder = os.path.join(root, dir)
                subfolder_files = [
                    f
                    for f in os.listdir(subfolder)
                    if os.path.isfile(os.path.join(subfolder, f))
                ]
                relative_path = os.path.relpath(subfolder, folder)
                subfolders.append(
                    {
                        "folder": os.path.join(root, dir),
                        "relative": relative_path,
                        "files": subfolder_files,
                    }
                )
        return subfolders

    def purge_string(self, s: str, replace_char: str = "_") -> str:
        """Purge input string of all special characters

        :param s: input string
        :type s: str
        :param replace_char: replacement, defaults to "_"
        :type replace_char: str, optional
        :return: purged string
        :rtype: str
        """
        s = replace_char.join(s.split())
        for ctr in self.CHAR_TO_REPLACE:
            s = s.replace(ctr, replace_char)

        rx = re.compile(r"_{2,}")
        s = rx.sub("", s)
        return s

    def remap(self, subfolder: Sequence[Dict]) -> Dict:
        """remap subfolders to underfolder

        :param subfolder: list of subfolders
        :type subfolder: Sequence[Dict]
        :return: list of single items files with metadata
        :rtype: Sequence[Dict]
        """
        items = []
        classmap = {}
        for sub_index, sub in enumerate(subfolder):
            for file in sub["files"]:
                file_path = os.path.join(sub["folder"], file)

                # ignore hidden files
                if Path(file_path).name.startswith("."):
                    continue

                category = self.purge_string(sub["relative"])
                data = {
                    "filename": file,
                    "filepath": file_path,
                    "category": category,
                }
                category_item = {
                    "name": category,
                    "class_id": sub_index,
                    "color": "#ff0000",
                }
                classmap[category] = category_item
                items.append(data)

        return {
            "items": items,
            "classmap": {"classes": [v for k, v in classmap.items()]},
        }

    def convert(self, output_folder: str):
        """

        :param output_folder: [description]
        :type output_folder: str
        """

        output_folder = Path(output_folder)
        if not output_folder.exists():
            output_folder.mkdir(parents=True, exist_ok=False)

        samples = []
        for index, item in enumerate(self._items):
            sample = FileSystemSample(data_map={}, id=index)
            sample.filesmap["image"] = item["filepath"]
            sample["metadata"] = {
                "category": item["category"],
                "filename": item["filename"],
            }
            sample["classmap"] = self._classmap
            samples.append(sample)

        writer = UnderfolderWriterV2(
            folder=output_folder,
            file_handling=UnderfolderWriterV2.FileHandling.COPY_IF_NOT_CACHED,
            copy_mode=UnderfolderWriterV2.CopyMode.HARD_LINK,
            reader_template=ReaderTemplate(
                extensions_map={
                    "image": self._images_extension,
                    "metadata": "yml",
                    "classmap": "yml",
                },
                root_files_keys=["classmap"],
            ),
            num_workers=self._num_workers,
            progress_callback=self._progress_callback,
        )
        writer(SamplesSequence(samples=samples))
