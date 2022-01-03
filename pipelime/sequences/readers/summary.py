from __future__ import annotations

from typing import Any, Iterable, Sequence, Type

import numpy as np

from pipelime.sequences.readers.base import BaseReader
from pipelime.sequences.samples import SamplesSequence


class ItemInfo:
    """Holds information on an Item"""

    def __init__(
        self,
        name: str,
        typeinfo: TypeInfo,
        count: int,
        root_item: bool,
        encoding: str,
    ) -> None:
        self.name = name
        self.typeinfo = typeinfo
        self.count = count
        self.root_item = root_item
        self.encoding = encoding


class TypeInfo:
    """Wraps the type of an Item, and handles the common properties of
    array-like types, such as shape and dtype.
    """

    def __init__(
        self,
        *types: Iterable[Type],
        shape: Iterable[int] = None,
        dtype: Any = None,
    ) -> None:
        self.types = set(types)
        self.shape = shape
        self.dtype = dtype

    def __repr__(self) -> str:
        return str((self.types, self.shape, self.dtype))

    def _merge(self, other: TypeInfo) -> TypeInfo:
        shape = self._merge_shapes(other)
        dtype = None
        if self.dtype == other.dtype:
            dtype = self.dtype
        return TypeInfo(*self.types, *other.types, shape=shape, dtype=dtype)

    def _merge_shapes(self, other):
        if self.shape is None or other.shape is None:
            return None
        if len(self.shape) != len(other.shape):
            return None

        a = np.array(self.shape)
        b = np.array(other.shape)

        where_eq = a == b
        final_shape = a * where_eq - 1 * (1 - where_eq)
        return final_shape.tolist()

    def __add__(self, other: TypeInfo) -> TypeInfo:
        """Creates a Union TypeInfo 'merging' the information of two
        TypeInfo objects.

        Output types is the set union of self types and other types

        If both objects have a specified shape with the same length, the
        output shape is the union shape, None otherwise.

        If both objects have a specified dtype with the same value, the
        output dtype is self.dtype, None otherwise.

        :param other: another TypeInfo object to merge
        :type other: TypeInfo
        :return: the union TypeInfo object
        :rtype: TypeInfo
        """
        return self._merge(other)


class ItemInfoFactory:
    """Provides an easy way to instantiate ItemInfo objects from a pipelime
    BaseReader object
    """

    @classmethod
    def _build_type_info(cls, obj: Any) -> TypeInfo:
        the_type = type(obj)
        shape = None
        dtype = None
        if hasattr(obj, "shape"):
            shape = obj.shape
        if hasattr(obj, "dtype"):
            dtype = str(obj.dtype)
        typeinfo = TypeInfo(the_type, shape=shape, dtype=dtype)
        return typeinfo

    @classmethod
    def build_item_info(cls, reader: BaseReader, key: str, k: int) -> ItemInfo:
        """Instantiates an ItemInfo object from the first k items whose key match the
        input key.

        :param reader: Input pipelime reader
        :type reader: BaseReader
        :param key: The query key to inspect
        :type key: str
        :param k: The first k samples to inspect, this is done to avoid reading the
        entire dataset and speedup the summary creation. Set `k<1` to inspect every
        sample.
        :type k: int
        :return: An ItemInfo object
        :rtype: ItemInfo
        """
        template = reader.get_reader_template()

        # Count items
        count = 0
        for sample in reader:
            if key in sample:
                count += 1

        # Get item types
        if k < 1:
            k = len(reader)
        typeinfos = []
        for i in range(k):
            sample = reader[i]
            if key in sample:
                typeinfo = cls._build_type_info(sample[key])
                typeinfos.append(typeinfo)
            if hasattr(sample, "flush"):
                sample.flush()
        typeinfo = typeinfos[0]
        for t in typeinfos:
            typeinfo += t

        # Is root item?
        root_item = key in template.root_files_keys

        # Get encoding type
        encoding = template.extensions_map.get(key)

        return ItemInfo(key, typeinfo, count, root_item, encoding)

    @classmethod
    def get_all_keys(cls, seq: SamplesSequence) -> Sequence[str]:
        """Retrieves all items contained in a SamplesSequence.

        :param seq: The sequence to inspect
        :type seq: SamplesSequence
        :return: The list of all item keys
        :rtype: Sequence[str]
        """
        all_keys = []
        for sample in seq:
            all_keys += list(sample.keys())
        all_keys = sorted(set(all_keys))
        return all_keys

    @classmethod
    def build_all(cls, reader: BaseReader, k: int) -> Sequence[ItemInfo]:
        """Creates all ItemInfo objects for every item key in an input reader

        :param reader: Input pipelime BaseReader
        :type reader: BaseReader
        :param k: The first k samples to inspect, this is done to avoid reading the
        entire dataset and speedup the summary creation. Set to k < 1 to inspect
        every sample.
        :type k: int
        :return: The list of all ItemInfos
        :rtype: Sequence[ItemInfo]
        """
        keys = cls.get_all_keys(reader)
        return [cls.build_item_info(reader, key, k) for key in keys]


class ReaderSummary:
    """Wraps a reader and its corresponding list of iteminfos"""

    def __init__(self, reader: BaseReader, k: int = 3) -> None:
        self.k = min(k, len(reader))
        self.reader = reader
        self._iteminfos = ItemInfoFactory.build_all(self.reader, self.k)

    def sort(self, key: str = "name", reverse: bool = False) -> None:
        """Internally sorts the iteminfo list according to a specified key

        :param key: the key along which to sort, defaults to "name"
        :type key: str, optional
        :param reverse: reverse sorting, defaults to False
        :type reverse: bool, optional
        """
        self._iteminfos = sorted(
            self._iteminfos, key=lambda x: str(getattr(x, key)), reverse=reverse
        )

    def __getitem__(self, k: int) -> ItemInfo:
        return self._iteminfos[k]

    def __len__(self) -> int:
        return len(self._iteminfos)
