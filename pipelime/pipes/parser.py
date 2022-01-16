import copy
from typing import Callable, Sequence, Tuple
import re
import pydash
import rich
from pipelime.tools.dictionaries import DictionaryUtils


class PipesConfigParser:

    PLACEHOLDER_CHAR = "@"
    PLACEHOLDER_REGEX = r = f"{PLACEHOLDER_CHAR}(\w*)\(([^)]+)\)"  # @methodName(arg)
    PLACEHOLDER_VARIABLE_NAME = "var"
    PLACEHOLDER_ITERATION_NAME = "iter"
    PLACEHOLDER_FOREACH_NAME = "foreach"
    PLACEHOLDER_FOREACH_DO_NAME = "do"
    PLACEHOLDER_FOREACH_ITEMS_NAME = "items"

    def __init__(self) -> None:
        self._regex = re.compile(self.PLACEHOLDER_REGEX)
        self._global_data = {}

    def set_global_data(self, key: str, value: any):
        """Set a global data key/value pair.

        :param key: key
        :type key: str
        :param value: value
        :type value: any
        """
        self._global_data[key] = value

    def _replace_variable(self, m: re.Match) -> any:
        """This is a callable, used in the _parse_string function. It will be called on
        every "value" found in the configuration dict.

        :param m: re.Match object
        :type m: re.Match
        :return: parsed value
        :rtype: any
        """
        command, content = m.groups()
        if command.lower() == PipesConfigParser.PLACEHOLDER_VARIABLE_NAME:
            value = pydash.get(self._global_data, content)
            return value
        else:
            return m.group()

    def _replace_foreach_item(self, m: re.Match, index: int, item: any) -> any:
        """This is a callable, used in the _parse_string function. It will be called on
        nodes in a foreach loop.

        :param m: re.Match object
        :type m: re.Match
        :param index: current index in the foreach loop
        :type index: int
        :param item: current item in the foreach loop
        :type item: any
        :return: parsed value
        :rtype: any
        """
        local_data = {"item": item, "index": index}
        command, content = m.groups()
        if command.lower() == PipesConfigParser.PLACEHOLDER_ITERATION_NAME:
            value = pydash.get(local_data, content)
            return value
        else:
            return m.group()

    def _parse_foreach_node(self, node: dict, item: any, index: int) -> dict:
        """Parse a foreach node (a dict). This is a node containing some @iter(index) and @iter(item)
        placeholder which will be replaced by the current item and index values in a foreach
        loop. For example a node could be a random dict like:

        {
            'data0': '@iter(item)',
            'data1': {
                'index': '@iter(index)',
            }
        }

        :param node: node to parse
        :type node: dict
        :param item: current item in the foreach loop
        :type item: any
        :param index: current index in the foreach loop
        :type index: int
        :return: parsed node
        :rtype: dict
        """
        empty = {}
        flat_node = DictionaryUtils.flatten(node)
        for key, value in flat_node.items():
            flat_node[key] = self._parse_string(
                value, lambda m: self._replace_foreach_item(m, index, item)
            )
            pydash.set_(empty, key, flat_node[key])
        return empty

    def _parse_string(
        self, s: str, replace_cb: Callable[[re.Match], any] = _replace_variable
    ) -> any:
        """Parse a string searching for placeholders. For example a string could be:

        'this is a string @var(params.input_folders) with a placeholder'

        The placeholder will be replaced by the value of the global data 'params.input_folders'.
        If the string contains multiple placeholders, the function will be called multiple times.

        If the string IS the placeholder itself (e.g. '@var(params.input_folders)'),
        the value of the global data will be returned and could be any type of data.

        :param s: string to parse
        :type s: str
        :param replace_cb: callable function responsible to build the repelcamente value
        given the input re.Match , defaults to lambdax:x.group()
        :type replace_cb: Callable[[re.Match], any], optional
        :return: parsed string. Could be a string or another type.
        :rtype: any
        """

        # How many matches?
        occurrences = len(self._regex.findall(s))

        # If one match and the match is the same as the string, return the value
        # this is used to replace values with any type other than a string. This is
        # possibile only if the whole input string is a placeholder.
        if occurrences == 1:
            match = next(self._regex.finditer(s))
            if match.group() == s:
                return replace_cb(match)

        # One or more matches among other random characters. This means that each match
        # can be replaced only with a string (a cast to string is done).
        if occurrences >= 1:
            matches = self._regex.finditer(s)
            repl_map = {}
            for match in matches:
                repl_map[match.group()] = str(replace_cb(match))
            return self._regex.sub(lambda m: repl_map[m.group()], s)

        # Nothing found. Return the original string.
        else:
            return s

    def _extract_foreach(self, node: dict) -> Tuple[dict, dict]:
        """Extract the foreach data from the node if any. A foreach node is a dict like:

        {
            'foreach': {
                'items': '@var(params.input_folders)',
                'do': {
                    'data0': '@iter(item)',
                    'data1': {
                        'index': '@iter(index)',
                    }
                }
            }
        }

        :param node: source node
        :type node: dict
        :raises Exception: If needed keys are missing ('do' and 'items')
        :return: (pseudo node, foreach data). The pseudo node is a dict within the 'do' key.
        'foreach data' is the data contained in the 'items' key.
        :rtype: Tuple[dict, dict]
        """
        if PipesConfigParser.PLACEHOLDER_FOREACH_NAME in node:
            foreach_node = node[PipesConfigParser.PLACEHOLDER_FOREACH_NAME]
            pseudo_node = foreach_node.get(
                PipesConfigParser.PLACEHOLDER_FOREACH_DO_NAME, None
            )
            foreach_data = foreach_node.get(
                PipesConfigParser.PLACEHOLDER_FOREACH_ITEMS_NAME, None
            )
            if pseudo_node is None or foreach_data is None:
                raise Exception("Invalid foreach node. Missing 'do' or 'items' keys.")
            return pseudo_node, foreach_data
        else:
            return None, None

    def _parse_and_replace_branch(self, cfg: dict) -> dict:
        """Parse a branch of the config. A branch is a dict several keys each of which
        could be a foreach node. This is used to parse foreach(s) inside nodes, like input
        or output ports. A branch could be:

        {
            'inputA': {
                'foreach': {
                    'items': ['a', 'b', 'c'],
                    'do': '@var(params.base_folder)/@iter(item)'
                    }
                }
            },
            'inputB': {
                'foreach': {
                    'items': ['a', 'b', 'c'],
                    'do': '@var(params.base_folder)/@iter(index)'
                    }
                }
            }
        }

        :param cfg: branch to parse
        :type cfg: dict
        :raises Exception: If needed keys are missing ('do' and 'items')
        :raises Exception: If the 'do' item is not a string
        :raises Exception: If the 'items' item is not a Sequence
        :return: parsed branch
        :rtype: dict
        """

        to_replace_data = {}
        for key, value in cfg.items():
            if isinstance(value, dict):
                if PipesConfigParser.PLACEHOLDER_FOREACH_NAME in value:
                    pseudo_node, foreach_data = self._extract_foreach(value)
                    if pseudo_node is not None:
                        if not isinstance(pseudo_node, str):
                            raise Exception(
                                "Invalid foreach node. 'do' key must be a sequence."
                            )
                        if not isinstance(foreach_data, Sequence):
                            raise Exception(
                                "Invalid foreach node. 'items' key must be a string."
                            )
                        parsed_list = []
                        for value_index, value in enumerate(foreach_data):
                            parsed_list.append(
                                self._parse_string(
                                    pseudo_node,
                                    lambda m: self._replace_foreach_item(
                                        m, value_index, value
                                    ),
                                )
                            )
                        to_replace_data[key] = parsed_list

        for key, value in to_replace_data.items():
            cfg[key] = value

        return cfg

    def parse_cfg(self, cfg: dict) -> dict:
        """Parse the configuration

        :param cfg: configuration to parse
        :type cfg: dict
        :return: parsed configuration
        :rtype: dict
        """

        parsed = copy.deepcopy(cfg)

        # Parse variables. This is done first because the variables could be used in
        # other nodes/foreach nodes. Iterates over the whole configuration as a flat list
        # of pydash(ed) keys and values
        for key, value in DictionaryUtils.flatten(cfg).items():
            if isinstance(value, str):
                pydash.set_(
                    parsed,
                    key,
                    self._parse_string(value, replace_cb=self._replace_variable),
                )

        # Parse foreach nodes. Each configuration node could contain a foreach node. This
        # means that the node generates multiple nodes based on a list of values.
        nodes = parsed["nodes"]
        to_add_nodes = {}  # used to not modify original dict while iterating
        to_delete_nodes = set()  # used to not modify original dict while iterating
        for node_name in nodes:
            node = nodes[node_name]
            pseudo_node, foreach_data = self._extract_foreach(node)

            # if pseudo_node is not None, then the node is a foreach node!
            if pseudo_node is not None:

                # Iterate the foreach data and create a new node for each item
                for index, data in enumerate(foreach_data):
                    new_node_key = str(index)
                    if isinstance(data, str):
                        new_node_key = data

                    # Create a new node with a name based on original name and the index
                    to_add_nodes[
                        f"{node_name}@{new_node_key}"
                    ] = self._parse_foreach_node(pseudo_node, data, index)

                # forwards the node to be deleted
                to_delete_nodes.add(node_name)

        # remove the foreach nodes
        for node_name in to_delete_nodes:
            del nodes[node_name]

        # Add the new nodes to the original config
        for node_name in to_add_nodes:
            nodes[node_name] = to_add_nodes[node_name]

        # Parse the branches. AKA foreach nodes inside nodes inputs/outputs values.
        nodes = parsed["nodes"]
        for node_name in nodes:
            node = nodes[node_name]

            # Parse the 'inputs'
            inputs = node.get("inputs", None)
            if inputs is not None:
                self._parse_and_replace_branch(inputs)

            # Parse the 'outputs'
            outputs = node.get("outputs", None)
            if outputs is not None:
                self._parse_and_replace_branch(outputs)

        return parsed


class PipeGraph:
    class PipeGraphNode:
        def __init__(self, name: str, type: str = "operation"):
            self.name = name
            self.type = type

        @property
        def id(self):
            return f"{self.type}({self.name})"

        def __hash__(self) -> int:
            return hash(f"{self.type}({self.name})")

        def __repr__(self) -> str:
            return f"{self.type}({self.name})"

        def __eq__(self, o: object) -> bool:
            return self.id == o.id

    @classmethod
    def build_nodes_graph(cls, cfg: dict) -> dict:
        """Build a graph of the nodes.

        :param cfg: configuration to parse
        :type cfg: dict
        :return: nodes graph
        :rtype: dict
        """

        import networkx as nx

        g = nx.DiGraph()
        nodes = cfg["nodes"]

        for node_name, node in nodes.items():
            inputs = node.get("inputs", [])
            outputs = node.get("outputs", [])

            for input_key, input_value in inputs.items():
                if isinstance(input_value, str):
                    g.add_edge(
                        PipeGraph.PipeGraphNode(input_value, "data"),
                        PipeGraph.PipeGraphNode(node_name, "operation"),
                    )
                elif isinstance(input_value, Sequence):
                    [
                        g.add_edge(
                            PipeGraph.PipeGraphNode(x, "data"),
                            PipeGraph.PipeGraphNode(node_name, "operation"),
                        )
                        for x in input_value
                    ]
                else:
                    raise NotImplementedError(
                        f"input type {type(input_value)} not implemented"
                    )

            for output_key, output_value in outputs.items():
                if isinstance(output_value, str):
                    g.add_edge(
                        PipeGraph.PipeGraphNode(node_name, "operation"),
                        PipeGraph.PipeGraphNode(output_value, "data"),
                    )
                elif isinstance(output_value, Sequence):
                    [
                        g.add_edge(
                            PipeGraph.PipeGraphNode(node_name, "operation"),
                            PipeGraph.PipeGraphNode(x, "data"),
                        )
                        for x in output_value
                    ]
                else:
                    raise NotImplementedError(
                        f"input type {type(output_value)} not implemented"
                    )

        return g
