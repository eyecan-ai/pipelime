from pathlib import Path
import subprocess
from typing import Any, Dict, List, Sequence, Tuple
import rich
from pipelime.pipes.executors.base import NodeModelExecutionParser, NodesGraphExecutor
from pipelime.pipes.graph import GraphNodeOperation, DAGNodesGraph
from pipelime.pipes.model import NodeModel
from pipelime.pipes.piper import PiperNamespace
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.validation import OperationValidate, SampleSchema, SchemaLoader


class NaiveNodeModelExecutionParser(NodeModelExecutionParser):
    def _append_argument_to_chunks(
        self,
        chunks: Sequence[str],
        argument_name: str,
        value: Any,
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
                if isinstance(x, List):
                    self._append_argument_to_chunks(chunks, argument_name, tuple(x))
                else:
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

        if node_model.inputs is not None:
            for k, v in node_model.inputs.items():
                self._append_argument_to_chunks(chunks, k, v)

        if node_model.outputs is not None:
            for k, v in node_model.outputs.items():
                self._append_argument_to_chunks(chunks, k, v)

        if node_model.args is not None:
            for k, v in node_model.args.items():
                self._append_argument_to_chunks(chunks, k, v)

        return chunks


class NaiveGraphExecutor(NodesGraphExecutor):
    def __init__(self) -> None:
        super().__init__()
        self._validated_paths = set()

    def _validate_path(
        self,
        node_model: NodeModel,
        name: str,
        value: any,
        schema_file: str,
    ) -> bool:

        # Iterate over list of values if any
        if isinstance(value, list):
            [
                self._validate_path(
                    node_model,
                    name=name,
                    value=x,
                    schema_file=schema_file,
                )
                for x in value
            ]
            return

        # Check if value is a string
        if isinstance(value, str):
            path = value
        else:
            raise NotImplementedError(f"{type(value)} is not supported")

        # Avoid validating the same path twice
        if path in self._validated_paths:
            return

        rich.print(
            "Validating",
            "\n",
            node_model,
            "\n",
            name,
            "\n",
            path,
            "\n",
            schema_file,
        )
        if Path(path).is_dir():

            try:
                reader = UnderfolderReader(folder=path)
                if schema_file is not None:

                    schema_file = SchemaLoader.load(schema_file)
                    op = OperationValidate(sample_schema=schema_file)
                    op(reader)

                    # Add path to validated paths to avoid validating it twice
                    self._validated_paths.add(path)

            except FileNotFoundError:
                pass

    def _validate_node_inputs(self, node_model: NodeModel) -> bool:

        if node_model.inputs is not None:
            for input_name, value in node_model.inputs.items():
                self._validate_path(
                    node_model,
                    name=input_name,
                    value=value,
                    schema_file=node_model.get_input_schema(input_name),
                )

    def _validate_node_outputs(self, node_model: NodeModel) -> bool:

        if node_model.outputs is not None:
            for output_name, value in node_model.outputs.items():
                self._validate_path(
                    node_model,
                    name=output_name,
                    value=value,
                    schema_file=node_model.get_output_schema(output_name),
                )

    def exec(self, graph: DAGNodesGraph, token: str = "") -> bool:

        parser = NaiveNodeModelExecutionParser()
        self._validated_paths.clear()

        for layer in graph.build_execution_stack():
            for node in layer:
                node: GraphNodeOperation
                command_chunks: List = parser.build_command_chunks(
                    node_model=node.node_model
                )
                command_chunks.append(PiperNamespace.ARGUMENT_NAME_TOKEN)
                command_chunks.append(token)
                command = " ".join(command_chunks)
                rich.print("Exec", command)

                # Validate inputs before call
                self._validate_node_inputs(node.node_model)

                pipe = subprocess.Popen(
                    command_chunks,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )

                stdout, stderr = pipe.communicate()
                if pipe.returncode == 0:

                    # validate produced outputs
                    self._validate_node_outputs(node.node_model)

                else:
                    rich.print("[red]Node Failed[/red]:", node)
                    raise RuntimeError(stderr)
