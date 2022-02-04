from abc import ABC, abstractmethod

import numpy as np

from pipelime.pipes.graph import NodesGraph


class NodesGraphDrawer(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def draw(self, graph: NodesGraph) -> np.ndarray:
        raise NotImplementedError()
