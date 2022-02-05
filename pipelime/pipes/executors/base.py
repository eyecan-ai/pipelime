from abc import ABC, abstractmethod
from typing import Any, Sequence
from pipelime.pipes.graph import DAGNodesGraph
from pipelime.pipes.model import NodeModel


class NodeModelExecutionParser(ABC):
    """A NodeModelExecutionParser is an abstract class that should implement the parsing
    of a NodeModel into a bash command and arguments.
    """

    def __init__(self) -> None:
        pass

    @abstractmethod
    def build_command_chunks(self, node_model: NodeModel) -> Sequence[Any]:
        """Builds the command chunks for the given node model. Chunks are pieces of the command
        that usually are separated by spaces.

        Args:
            node_model (NodeModel): input node model

        Returns:
            Sequence[Any]: command chunks
        """
        raise NotImplementedError()

    def build_plain_command(self, node_model: NodeModel) -> str:
        """Builds the plain command for the given node model. It joins the command chunks with
        spaces.

        Args:
            node_model (NodeModel): input node model

        Returns:
            str: plain bash command
        """
        return " ".join(self.build_command_chunks(node_model=node_model))


class NodesGraphExecutor(ABC):
    """An NodesGraphExecutor is an abstract class that should implement the execution of a
    NodesGraph made of Nodes.

    """

    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def exec(self, graph: DAGNodesGraph) -> bool:
        """Executes the given NodesGraph.

        Args:
            graph (DAGNodesGraph): input DAGNodesGraph

        Returns:
            bool: TRUE if the execution was successful, FALSE otherwise
        """
        raise NotImplementedError()
