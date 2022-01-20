from pipelime.sequences.samples import Sample, FileSystemItem
from pipelime.sequences.stages import SampleStage, StageIdentity, StageKeysFilter
from pipelime.filesystem.toolkit import FSToolkit

import numpy as np
import imageio

import shutil
from enum import Enum
from pathlib import Path
from itertools import islice

from abc import ABC, abstractmethod
from typing import Union, Optional, Any, Sequence, Mapping, Collection, Tuple


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
        return HeaderDataWriter(self._prefix + f">{key}", self._sep)

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
            type_str = (
                "s"
                if isinstance(elem, (str, bytes))
                else ("d" if isinstance(elem, (int, bool)) else "c")
            )
        return self._sep + type_str


class HeaderDataRoleWriter(DataWriter):
    def __init__(
        self,
        node_name: str,
        sep: str,
        output_path: Union[Path, str],
        role_map: Mapping[str, str],
    ):
        self._node_name = node_name
        self._sep = sep
        self._output_path = str(output_path)
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
            elif isinstance(elem, (int, bool)):
                role_str = "class"
            else:
                role_str = ""
        return self._sep + role_str


class ValueDataWriter(DataWriter):
    def __init__(self, sep: str, output_path: Union[Path, str]):
        self._sep = sep
        self._output_path = Path(output_path)

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
    DEEP_COPY = 0
    SYM_LINK = 1
    HARD_LINK = 2


def _make_iter(
    samples: Sequence[Sample],
    start: int,
    stop: int,
    step: int,
):
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
    return islice(samples, start, stop, step)


def _link_file(
    src_path: Path,
    target_folder: Path,
    link_type: LinkType,
):
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


def _extract_samples(
    assets_path: Path,
    link_type: LinkType,
    samples: Sequence[Sample],
    stage: SampleStage,
    start: int,
    stop: int,
    step: int,
) -> Sequence[Mapping[str, Any]]:
    filtered_samples: Sequence[Mapping[str, Any]] = []
    for src in _make_iter(samples, start, stop, step):
        src = stage(src)
        dst: dict[str, Any] = {}
        for k in src:
            item = src.metaitem(k)

            # link images, load everything else
            dst_data = (
                _link_file(Path(item.source()), assets_path, link_type)
                if isinstance(item, FileSystemItem)
                and FSToolkit.is_image_file(str(item.source()))
                else src[k]  # this may load a file from disk
            )

            # dump image-like data to disk
            if (
                isinstance(dst_data, np.ndarray)
                and dst_data.dtype == "uint8"
                and (
                    len(dst_data.shape) == 2  # (H, W) mono
                    or (
                        len(dst_data.shape) == 3  # mono, rgb, rgba
                        and (
                            dst_data.shape[0] in (1, 3, 4)  # (C, H, W)
                            or dst_data.shape[-1] in (1, 3, 4)  # (H, W, C)
                        )
                    )
                )
            ):
                if len(dst_data.shape) == 3:
                    if dst_data.shape[0] in (1, 3, 4):
                        dst_data = np.moveaxis(dst_data, -3, -1)
                    if dst_data.shape[-1] == 1:
                        dst_data = np.squeeze(dst_data, axis=-1)
                trg_file = assets_path / Path(f"{len(filtered_samples)}-{k}.png")
                imageio.imwrite(trg_file, dst_data)
                dst_data = str(trg_file)

            # store in the destination map
            dst[k] = dst_data

        filtered_samples.append(dst)

    return filtered_samples


def _print_line(first_col: Any, others: str, file: Optional[Any]):
    print(first_col, end="", file=file)
    print(others, end="\n", file=file)


def dump_data(
    output_assets_path: Union[Path, str],
    samples: Sequence[Sample],
    key_filters: Optional[Sequence[str]] = None,
    negate_key_filter: bool = False,
    start: int = 0,
    stop: int = -1,
    step: int = 1,
    format: str = "orange",
    link_type: LinkType = LinkType.HARD_LINK,
    file: Optional[Any] = None,
) -> None:
    """Sample data dumping to console or file.

    Args:
        output_assets_path (Union[Path, str]): where images and assets will be saved.
        samples (Sequence[Sample]): the sequence of samples.
        key_filters (Optional[Sequence[str]], optional): an optional list of keys.
            Defaults to None.
        negate_key_filter (bool, optional): if True, keys in key_filters are excluded,
            otherwise are taken. Defaults to False.
        start (int, optional): first sample index. Defaults to 0.
        stop (int, optional): last sample index. Defaults to -1.
        step (int, optional): range step. Defaults to 1.
        format (str, optional): output format ('csv', 'orange'). Defaults to "orange".
        link_type (LinkType, optional): if soft/hard links should be used whenever
            possible. Defaults to LinkType.HARD_LINK.
        file (Optional[Any], optional): an optional opened file stream, if None it
            prints to stdout. Defaults to None.
    """
    output_assets_path = Path(output_assets_path).resolve()
    output_assets_path.mkdir(parents=True, exist_ok=True)

    filtered_samples = _extract_samples(
        output_assets_path,
        link_type,
        samples,
        StageIdentity()
        if key_filters is None
        else StageKeysFilter(key_list=key_filters, negate=negate_key_filter),
        start,
        stop,
        step,
    )

    format = format.lower()
    separator = "\t" if format == "orange" else ","
    samples_it = enumerate(filtered_samples)

    # write header from a prototype
    first_idx, first_sample = next(samples_it)

    _print_line(
        "$index",
        _dump_collection(first_sample, HeaderDataWriter(prefix="", sep=separator)),
        file,
    )

    if format == "orange":
        _print_line(
            "d",
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
                    node_name="", sep=separator, output_path=output_assets_path, role_map={}
                ),
            ),
            file,
        )

    # write values
    _print_line(
        first_idx,
        _dump_collection(
            first_sample, ValueDataWriter(sep=separator, output_path=output_assets_path)
        ),
        file,
    )

    for sample_idx, sample_data in samples_it:
        _print_line(
            sample_idx,
            _dump_collection(
                sample_data, ValueDataWriter(sep=separator, output_path=output_assets_path)
            ),
            file,
        )
