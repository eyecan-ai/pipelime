from abc import ABC, abstractmethod
from typing import Optional, Sequence
from pathlib import Path

import numpy as np

from pipelime.pipes.graph import DAGNodesGraph


class NodesGraphDrawer(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def exportable_formats(self) -> Sequence[str]:
        raise NotImplementedError()

    @abstractmethod
    def draw(self, graph: DAGNodesGraph) -> np.ndarray:
        raise NotImplementedError()

    @abstractmethod
    def representation(self, graph: DAGNodesGraph) -> str:
        raise NotImplementedError()

    @abstractmethod
    def export(self, graph: DAGNodesGraph, filename: str, format: Optional[str] = None):
        if format is None:
            format = Path(filename).suffix[1:]
        if format not in self.exportable_formats():
            raise ValueError(f"Format {format} not supported")
