from abc import ABC, abstractmethod
import os
from typing import Dict, Sequence
from pathlib import Path
import re


class UnderfolderConverter(ABC):
    CHAR_TO_REPLACE = ["/", "\\", ":", "*", "?", '"', "<", ">", "|", "-"]

    @abstractmethod
    def convert(self, output_folder: str):
        raise NotImplementedError

    @abstractmethod
    def extensions_map(self) -> dict:
        pass

    @abstractmethod
    def root_files_keys(self) -> dict:
        pass

    def extract_items_and_classmap(self, folder: str) -> Dict[str, dict]:
        return self._remap(self._extract_subfolders_and_files(folder=folder))

    def _extract_subfolders_and_files(self, folder: str) -> Sequence[Dict]:
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

    def _remap(self, subfolder: Sequence[Dict]) -> Dict:
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

    def _extract_samples_map(self, items: Dict) -> Dict:
        """Input items like

        [
            {
                'filename':...
                'filepath':...
                'category : ...
            }
        ]

        where filename could have the same base "name" e.g ALPHA.png ALPHA.txt share the
        same base name "ALPHA"

        :param items: input items
        :type items: Dict
        :return: merged items
        :rtype: Dict
        """

        samples_map = {}

        for item in items:
            path = Path(item["filepath"])

            key = str(path.parent / path.stem)

            suffix = path.suffix.lstrip(".")

            if key not in samples_map:
                samples_map[key] = {}

            if suffix in samples_map[key]:
                raise ValueError(f'Duplicate sample "{key}.{suffix}"')

            samples_map[key][suffix] = item

        return samples_map
