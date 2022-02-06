from abc import ABC, abstractmethod

import numpy as np

from pipelime.pipes.graph import DAGNodesGraph


class NodesGraphDrawer(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def draw(self, graph: DAGNodesGraph) -> np.ndarray:
        raise NotImplementedError()
