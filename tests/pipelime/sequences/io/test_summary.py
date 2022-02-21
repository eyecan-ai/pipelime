from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.readers.summary import ReaderSummary, TypeInfo
import numpy as np


class TestSummary:
    def test_summary(self, toy_dataset_small):
        path = toy_dataset_small["folder"]
        non_root = toy_dataset_small["expected_keys"]
        root_keys = toy_dataset_small["root_keys"]
        all_keys = [*non_root, *root_keys]
        reader = UnderfolderReader(path)
        summary = ReaderSummary(reader, k=-1)
        summary.sort()
        assert len(summary) == len(all_keys)

        # Check iteminfo 0
        iteminfo = summary[0]
        key = sorted(all_keys)[0]
        assert iteminfo.name == key
        assert iteminfo.count == len(reader)
        assert np.ndarray in iteminfo.typeinfo.types
        assert iteminfo.root_item == (key in root_keys)
        assert iteminfo.encoding == "npy"


class TestTypeInfo:
    def test_typeinfo(self):
        # Compatible but different shape
        a = TypeInfo(np.ndarray, shape=[100, 100, 100], dtype="uint8")
        b = TypeInfo(np.ndarray, shape=[200, 100, 100], dtype="uint8")
        c = a + b
        assert c.types == {np.ndarray}
        assert c.shape == [-1, 100, 100]
        assert c.dtype == "uint8"

        # Incompatible shape
        a = TypeInfo(np.ndarray, shape=[100, 100, 100], dtype="uint8")
        b = TypeInfo(np.ndarray, shape=[100, 100], dtype="uint8")
        c = a + b
        assert c.types == {np.ndarray}
        assert c.shape is None
        assert c.dtype == "uint8"

        # None shape
        a = TypeInfo(np.ndarray, shape=None, dtype="uint8")
        b = TypeInfo(np.ndarray, shape=[200, 100, 100], dtype="uint8")
        c = a + b
        assert c.types == {np.ndarray}
        assert c.shape is None
        assert c.dtype == "uint8"

        # Incompatible dtypes
        a = TypeInfo(np.ndarray, shape=None, dtype="uint16")
        b = TypeInfo(np.ndarray, shape=None, dtype="uint8")
        c = a + b
        assert c.types == {np.ndarray}
        assert c.shape is None
        assert c.dtype is None

        # Incompatible types
        a = TypeInfo(list, shape=None, dtype="uint8")
        b = TypeInfo(np.ndarray, shape=[200, 100, 100], dtype="uint8")
        c = a + b
        assert c.types == {np.ndarray, list}
        assert c.shape is None
        assert c.dtype == "uint8"
