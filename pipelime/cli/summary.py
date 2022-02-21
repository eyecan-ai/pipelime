from __future__ import annotations

from rich.console import Console
from rich.table import Table

from pipelime.sequences.readers.base import BaseReader
from pipelime.sequences.readers.summary import ItemInfo, ReaderSummary, TypeInfo
from pipelime.sequences.samples import SamplesSequence
from pipelime.tools.bytes import DataCoding


class SummaryPrinter:
    TITLE_STYLE = "bold bright_blue"
    ENC_CAT_STYLE_MAP = {
        "image": "bold magenta",
        "numpy": "bold cyan",
        "markup": "bold yellow",
        "pickle": "bold red",
    }
    ENC_CAT_MAP = {
        "image": DataCoding.IMAGE_CODECS,
        "numpy": DataCoding.NUMPY_CODECS + DataCoding.TEXT_CODECS,
        "markup": DataCoding.METADATA_CODECS,
        "pickle": DataCoding.PICKLE_CODECS,
    }
    ENC_STYLE_MAP = {}
    for c in ENC_CAT_MAP:
        for codec in ENC_CAT_MAP[c]:
            ENC_STYLE_MAP[codec] = ENC_CAT_STYLE_MAP[c]
    ITEM_NAME_STYLE = "bold blue"
    TYPE_NAME_STYLE = "bold green"
    DTYPE_STYLE = "green"
    SHAPE_STYLE = "yellow"
    BOOL_TRUE_STYLE = "bold green"
    BOOL_FALSE_STYLE = "bold red"

    def repr_name(self, name: str) -> str:
        return f"[{self.ITEM_NAME_STYLE}]{name}[/{self.ITEM_NAME_STYLE}]"

    def repr_typeinfo(self, t: TypeInfo) -> str:
        style = self.TYPE_NAME_STYLE
        typenames = [f"[{style}]{x.__name__}[/{style}]" for x in t.types]
        str_ = " | ".join(typenames)
        if t.dtype is not None:
            str_ += f" [{self.DTYPE_STYLE}]{t.dtype}[/{self.DTYPE_STYLE}]"
        if t.shape is not None:
            style = self.SHAPE_STYLE
            formatted_shape = [f"[{style}]{x}[/{style}]" for x in t.shape]
            str_ += " [" + " × ".join(formatted_shape) + "]"
        return str_

    def repr_count(self, c: int, total: int) -> str:
        style = self.BOOL_TRUE_STYLE if c == total else self.BOOL_FALSE_STYLE
        return f"[{style}]{c}/{total}[/{style}]"

    def repr_bool(self, val: bool) -> str:
        style = self.BOOL_TRUE_STYLE if val else self.BOOL_FALSE_STYLE
        return f"[{style}]{val}[/{style}]"

    def repr_encoding(self, enc: str) -> str:
        if enc not in self.ENC_STYLE_MAP:
            return "Unknown"
        style = self.ENC_STYLE_MAP[enc]
        return f"[{style}]{enc}[/{style}]"

    def repr_iteminfo(self, seq: SamplesSequence, iteminfo: ItemInfo) -> str:
        repr_name_ = self.repr_name(iteminfo.name)
        repr_typeinfo_ = self.repr_typeinfo(iteminfo.typeinfo)
        repr_count_ = self.repr_count(iteminfo.count, len(seq))
        repr_is_root_ = self.repr_bool(iteminfo.root_item)
        repr_encoding_ = self.repr_encoding(iteminfo.encoding)
        return repr_name_, repr_count_, repr_typeinfo_, repr_is_root_, repr_encoding_

    def print(self, summary: ReaderSummary) -> None:
        console = Console()
        style = self.TITLE_STYLE
        console.print(f"[{style}]Type[/{style}]: {summary.reader.__class__.__name__}")
        console.print(f"[{style}]Length[/{style}]: {len(summary.reader)}")
        console.print()
        if summary.k > 1:
            str_ = f"based on first {summary.k} samples"
        elif summary.k == 1:
            str_ = "based on first sample"
        else:
            str_ = "based on all samples"
        console.print(f"[{style}]Contents[/{style}] \\[{str_}]:")

        table = Table(header_style=self.TITLE_STYLE)
        table.add_column("Item Name")
        table.add_column("Count")
        table.add_column("Item Type")
        table.add_column("Is Root Item")
        table.add_column("Encoding")

        for iteminfo in summary:
            repr_iteminfo_ = self.repr_iteminfo(summary.reader, iteminfo)
            table.add_row(*repr_iteminfo_)

        console.print(table)
        pass


def print_summary(
    reader: BaseReader,
    max_samples: int = 3,
    order_by: str = "name",
    reversed_: bool = False,
) -> None:
    summary = ReaderSummary(reader, k=max_samples)
    summary.sort(key=order_by, reverse=reversed_)
    SummaryPrinter().print(summary)
