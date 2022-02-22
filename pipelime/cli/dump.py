from pipelime.sequences.samples import Sample, FileSystemItem
from pipelime.filesystem.toolkit import FSToolkit
from pipelime.pipes.piper import PiperCommand
from pipelime.tools.progress import pipelime_track

import numpy as np
import imageio

import shutil
from enum import Enum
from pathlib import Path
from itertools import islice

from abc import ABC, abstractmethod
from typing import Union, Optional, Any, Sequence, Mapping, Collection, Tuple, Callable


class DataWriter(ABC):
    @abstractmethod
    def index_node(self, idx: int) -> "DataWriter":
        ...

    @abstractmethod
    def key_node(self, key: str) -> "DataWriter":
        ...

    @abstractmethod
    def coordinate_node(self, coord: Tuple) -> "DataWriter":
        ...

    @abstractmethod
    def dump_leaf(self, elem) -> str:
        ...


class HeaderDataWriter(DataWriter):
    def __init__(self, prefix: str, sep: str):
        self._prefix = prefix
        self._sep = sep

    def index_node(self, idx: int) -> DataWriter:
        return HeaderDataWriter(self._prefix + f"[{idx}]", self._sep)

    def key_node(self, key: str) -> DataWriter:
        return HeaderDataWriter(
            self._prefix + f">{key}" if self._prefix else f"{key}", self._sep
        )

    def coordinate_node(self, coords: Tuple) -> DataWriter:
        coord_str = ":".join([str(idx) for idx in coords])
        return HeaderDataWriter(self._prefix + f"({coord_str})", self._sep)

    def dump_leaf(self, elem) -> str:
        return self._sep + self._prefix


class HeaderDataTypeWriter(DataWriter):
    def __init__(self, node_name: str, sep: str, type_map: Mapping[str, str]):
        self._node_name = node_name
        self._sep = sep
        self._type_map = type_map

    def index_node(self, idx: int) -> DataWriter:
        return HeaderDataTypeWriter(
            self._node_name + f"#{idx}", self._sep, self._type_map
        )

    def key_node(self, key: str) -> DataWriter:
        return HeaderDataTypeWriter(
            self._node_name + f"${key}", self._sep, self._type_map
        )

    def coordinate_node(self, coords: Tuple) -> DataWriter:
        coord_str = ":".join([str(idx) for idx in coords])
        return HeaderDataTypeWriter(
            self._node_name + f"@{coord_str}", self._sep, self._type_map
        )

    def dump_leaf(self, elem) -> str:
        type_str = self._type_map.get(self._node_name)
        if type_str is None:
            type_str = "s" if isinstance(elem, (str, bytes)) else "c"
        return self._sep + type_str


class HeaderDataRoleWriter(DataWriter):
    def __init__(
        self,
        node_name: str,
        sep: str,
        output_path: Optional[Union[Path, str]],
        role_map: Mapping[str, str],
    ):
        self._node_name = node_name
        self._sep = sep
        self._output_path = "" if output_path is None else str(output_path)
        self._role_map = role_map

    def index_node(self, idx: int) -> DataWriter:
        return HeaderDataRoleWriter(
            self._node_name + f"#{idx}", self._sep, self._output_path, self._role_map
        )

    def key_node(self, key: str) -> DataWriter:
        return HeaderDataRoleWriter(
            self._node_name + f"${key}", self._sep, self._output_path, self._role_map
        )

    def coordinate_node(self, coords: Tuple) -> DataWriter:
        coord_str = ":".join([str(idx) for idx in coords])
        return HeaderDataRoleWriter(
            self._node_name + f"@{coord_str}",
            self._sep,
            self._output_path,
            self._role_map,
        )

    def dump_leaf(self, elem) -> str:
        role_str = self._role_map.get(self._node_name)
        if role_str is None:
            if isinstance(elem, (str, bytes)):
                elem = str(elem)
                if Path(elem).is_file() and FSToolkit.is_image_file(elem):
                    role_str = f"type=image origin={self._output_path}"
                else:
                    role_str = "meta"
            else:
                role_str = ""
        return self._sep + role_str


class ValueDataWriter(DataWriter):
    def __init__(self, sep: str, output_path: Optional[Union[Path, str]]):
        self._sep = sep
        self._output_path = None if output_path is None else Path(output_path)

    def index_node(self, idx: int) -> DataWriter:
        return ValueDataWriter(self._sep, self._output_path)

    def key_node(self, key: str) -> DataWriter:
        return ValueDataWriter(self._sep, self._output_path)

    def coordinate_node(self, coord: Tuple) -> DataWriter:
        return ValueDataWriter(self._sep, self._output_path)

    def dump_leaf(self, elem) -> str:
        if isinstance(elem, (str, bytes)):
            elem = str(elem)
            if Path(elem).is_file() and FSToolkit.is_image_file(elem):
                elem = Path(elem)
                if self._output_path is not None:
                    elem = str(elem.relative_to(self._output_path))
        return self._sep + str(elem)


def _dump_collection(x: Any, writer: DataWriter) -> str:
    if isinstance(x, Collection) and not isinstance(x, (str, bytes)):
        outline = ""
        if isinstance(x, Mapping):
            for key, elem in x.items():
                outline += _dump_collection(elem, writer.key_node(key))
        elif isinstance(x, np.ndarray):
            it = np.nditer(x, flags=["multi_index"])
            for elem in it:
                outline += _dump_collection(
                    elem.item(), writer.coordinate_node(it.multi_index)  # type: ignore
                )
        else:
            for idx, elem in enumerate(x):
                outline += _dump_collection(elem, writer.index_node(idx))
        return outline
    else:
        return writer.dump_leaf(x)


class LinkType(Enum):
    DEEP_COPY = "deepcopy"
    SYM_LINK = "symlink"
    HARD_LINK = "hardlink"


def _make_sample_iterator(
    samples: Sequence[Sample],
    start: Optional[int],
    stop: Optional[int],
    step: Optional[int],
):
    index_range = range(len(samples))
    if step is None:
        step = 1
    if start is None:
        start = 0 if step > 0 else -1
    if start < 0:
        start = len(samples) + start
    if stop is not None and stop < 0:
        stop = len(samples) + stop
    if step < 0:
        start = len(samples) - 1 - start
        if stop is not None:
            stop = len(samples) - 1 - stop
        step = -step
        samples = reversed(samples)  # type: ignore
        index_range = reversed(index_range)

    from math import trunc

    return zip(
        islice(index_range, start, stop, step), islice(samples, start, stop, step)
    ), trunc(((len(samples) if stop is None else stop) - 1 - start) / step + 1)


def _link_file(
    src_path: Path, target_folder: Optional[Path], link_type: LinkType
) -> str:
    if target_folder is None or not target_folder.exists():
        return str((Path("") / src_path.name).resolve())  # fake path

    trg_file = target_folder / src_path.name

    if link_type is LinkType.DEEP_COPY:
        shutil.copy(src_path, str(trg_file))
    elif link_type is LinkType.SYM_LINK:
        trg_file.symlink_to(src_path)
    elif link_type is LinkType.HARD_LINK:
        try:
            # (new in version 3.10)
            trg_file.hardlink_to(src_path)  # type: ignore
        except AttributeError:
            import os

            os.link(str(src_path), str(trg_file))

    return str(trg_file)


def _write_image(
    image: np.ndarray, filename: str, target_folder: Optional[Path]
) -> str:
    if target_folder is None or not target_folder.exists():
        return str((Path("") / filename).resolve())  # fake path

    trg_file = target_folder / Path(filename)
    imageio.imwrite(trg_file, image)
    return str(trg_file)


def _link_or_load(
    sample: Sample, item_key: str, target_folder: Optional[Path], link_type: LinkType
) -> Any:
    # link images, load everything else
    item = sample.metaitem(item_key)
    return (
        _link_file(Path(item.source()), target_folder, link_type)
        if isinstance(item, FileSystemItem)
        and FSToolkit.is_image_file(str(item.source()))
        else sample[item_key]  # this may load a file from disk
    )


def _maybe_write_data(
    data: Any, sample_index: int, item_key: str, target_folder: Optional[Path]
) -> Any:
    # dump image-like data to disk
    if (
        isinstance(data, np.ndarray)
        and data.dtype == "uint8"
        and (
            len(data.shape) == 2  # (H, W) mono
            or (
                len(data.shape) == 3  # mono, rgb, rgba
                and (
                    data.shape[0] in (1, 3, 4)  # (C, H, W)
                    or data.shape[-1] in (1, 3, 4)  # (H, W, C)
                )
            )
        )
    ):
        if len(data.shape) == 3:
            if data.shape[0] in (1, 3, 4):
                data = np.moveaxis(data, -3, -1)
            if data.shape[-1] == 1:
                data = np.squeeze(data, axis=-1)
        return _write_image(data, f"{sample_index}-{item_key}.png", target_folder)

    return data


def _make_track(
    progress_callback: Optional[Callable[[dict], None]] = None, *args, **kwargs
):
    return (
        pipelime_track(*args, track_callback=progress_callback, **kwargs)
        if progress_callback is None
        else pipelime_track(
            *args, track_callback=progress_callback, disable=True, **kwargs
        )
    )


def _extract_samples(
    assets_path: Optional[Path],
    link_type: LinkType,
    samples: Sequence[Sample],
    start: Optional[int],
    stop: Optional[int],
    step: Optional[int],
    progress_callback: Optional[Callable[[dict], None]],
) -> Sequence[Tuple[int, Mapping[str, Any]]]:
    filtered_samples: Sequence[Tuple[int, Mapping[str, Any]]] = []
    sample_it, size = _make_sample_iterator(samples, start, stop, step)

    for index, src in _make_track(
        progress_callback, sample_it, total=size, description="Reading samples..."
    ):
        dst: dict[str, Any] = {}
        for k in src:
            dst_data = _link_or_load(src, k, assets_path, link_type)
            dst[k] = _maybe_write_data(dst_data, len(filtered_samples), k, assets_path)

        filtered_samples.append((index, dst))

    return filtered_samples


def _print_line(first_col: Any, others: str, file: Optional[Any]):
    print(first_col, end="", file=file)
    print(others, end="\n", file=file)


def dump_data(
    samples: Sequence[Sample],
    output_assets_path: Optional[Union[Path, str]] = None,
    start: Optional[int] = None,
    stop: Optional[int] = None,
    step: Optional[int] = None,
    format: str = "orange",
    link_type: LinkType = LinkType.HARD_LINK,
    file: Optional[Any] = None,
    piper_command: Optional[PiperCommand] = None,
) -> None:
    """Sample data dumping to console or file.

    Args:
        samples (Sequence[Sample]): the sequence of samples.
        output_assets_path (Optional[Union[Path, str]]): where images and other file
            assets will be saved.
        start (Optional[int], optional): first sample index. Defaults to None.
        stop (Optional[int], optional): last sample index (excluded). Defaults to None.
        step (Optional[int], optional): range step. Defaults to None.
        format (str, optional): output format ('csv', 'orange'). Defaults to 'orange'.
        link_type (LinkType, optional): if soft/hard links should be used whenever
            possible when writing assets. Defaults to LinkType.HARD_LINK.
        file (Optional[Any], optional): an optional opened file stream, if None it
            prints to stdout. Defaults to None.
        piper_command (Optional[PiperCommand], optional): the parent piper command, if
            any. Defaults to None.
    """
    if output_assets_path is not None:
        output_assets_path = Path(output_assets_path).resolve()
        output_assets_path.mkdir(parents=True, exist_ok=True)

    filtered_samples = _extract_samples(
        output_assets_path,
        link_type,
        samples,
        start,
        stop,
        step,
        None
        if piper_command is None
        else piper_command.generate_progress_callback(0, 2),
    )

    tracked_data = iter(
        _make_track(
            None
            if piper_command is None
            else piper_command.generate_progress_callback(0, 2),
            filtered_samples,
            total=len(filtered_samples),
            description="Writing data...",
        )
    )

    # write header from a prototype
    format = format.lower()
    separator = "\t" if format == "orange" else ","

    first_idx, first_sample = next(tracked_data)

    _print_line(
        "$index",
        _dump_collection(first_sample, HeaderDataWriter(prefix="", sep=separator)),
        file,
    )

    if format == "orange":
        _print_line(
            "c",
            _dump_collection(
                first_sample,
                HeaderDataTypeWriter(node_name="", sep=separator, type_map={}),
            ),
            file,
        )
        _print_line(
            "meta",
            _dump_collection(
                first_sample,
                HeaderDataRoleWriter(
                    node_name="",
                    sep=separator,
                    output_path=output_assets_path,
                    role_map={},
                ),
            ),
            file,
        )

    # write values
    _print_line(
        first_idx,
        _dump_collection(
            first_sample,
            ValueDataWriter(sep=separator, output_path=output_assets_path),
        ),
        file,
    )

    for sample_idx, sample_data in tracked_data:
        _print_line(
            sample_idx,
            _dump_collection(
                sample_data,
                ValueDataWriter(sep=separator, output_path=output_assets_path),
            ),
            file,
        )
