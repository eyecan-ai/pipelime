from abc import ABC, abstractmethod
import subprocess
from typing import Any, Dict, List, Sequence, Tuple

import rich
from pipelime.pipes.graph import GraphNodeOperation, NodesGraph

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
    def exec(self, graph: NodesGraph) -> bool:
        """Executes the given NodesGraph.

        Args:
            graph (NodesGraph): input NodesGraph

        Returns:
            bool: TRUE if the execution was successful, FALSE otherwise
        """
        raise NotImplementedError()


###############################################################################
# NAIVE
###############################################################################


class NaiveNodeModelExecutionParser(NodeModelExecutionParser):
    def _append_argument_to_chunks(
        self, chunks: Sequence[str], argument_name: str, value: Any
    ):
        """Appends the given argument to the given chunks list. It manages different
        types of values.

        Args:
            chunks (Sequence[str]): input chunks list [in/out]
            argument_name (str): input argument name
            value (Any): input argument value
        """
        if isinstance(value, List):
            for x in value:
                self._append_argument_to_chunks(chunks, argument_name, x)
        elif isinstance(value, Tuple):
            chunks.append(f"--{argument_name}")
            for t in value:
                chunks.append(str(t))
        elif isinstance(value, Dict):
            chunks.append(f"--{argument_name}")
            for k, v in value.items():
                chunks.append(str(k))
                chunks.append(str(v))
        else:
            chunks.append(f"--{argument_name}")
            chunks.append(str(value))

    def build_command_chunks(self, node_model: NodeModel) -> Sequence[Any]:
        chunks = node_model.command.split(" ")
        for k, v in node_model.inputs.items():
            self._append_argument_to_chunks(chunks, k, v)
        for k, v in node_model.outputs.items():
            self._append_argument_to_chunks(chunks, k, v)
        for k, v in node_model.args.items():
            self._append_argument_to_chunks(chunks, k, v)
        return chunks


class NaiveGraphExecutor(NodesGraphExecutor):
    def __init__(self) -> None:
        super().__init__()

    def exec(self, graph: NodesGraph) -> bool:

        parser = NaiveNodeModelExecutionParser()

        for layer in graph.build_execution_stack():
            print("Layer")
            processes = []
            for node in layer:
                node: GraphNodeOperation
                command_chunks = parser.build_command_chunks(node_model=node.node_model)
                command = parser.build_plain_command(node_model=node.node_model)
                rich.print("Exec", command)

                try:
                    subprocess.check_call(
                        command_chunks,
                        shell=False,
                    )
                except Exception as e:
                    rich.print("[red]Node Failed[/red]:", node)
                    raise RuntimeError()

    # def build_full_command(self) -> str:
    #     return " ".join(self.build_chunks())
