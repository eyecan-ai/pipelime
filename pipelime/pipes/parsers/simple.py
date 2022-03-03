import copy
from typing import Callable, Optional, Sequence, Tuple
import re
import pydash
from pipelime.pipes.parsers.base import DAGConfigParser
from pipelime.tools.dictionaries import DictionaryUtils
from pipelime.pipes.model import DAGModel


class DAGSimpleParser(DAGConfigParser):

    PLACEHOLDER_CHAR = "\$"
    PLACEHOLDER_REGEX = r = f"{PLACEHOLDER_CHAR}(\\w*)\\(([^)]+)\\)"  # @methodName(arg)
    PLACEHOLDER_VARIABLE_NAME = "var"
    PLACEHOLDER_ITERATION_NAME = "iter"
    PLACEHOLDER_ARG_ITERATION_NAME = "argiter"
    PLACEHOLDER_FOREACH_NAME = "foreach"
    PLACEHOLDER_FOREACHARG_NAME = "foreach_arg"
    PLACEHOLDER_FOREACH_DO_NAME = "do"
    PLACEHOLDER_FOREACH_ITEMS_NAME = "items"
    PLACEHOLDER_ARG_SPLIT_CHAR = "@"

    def __init__(self) -> None:
        self._regex = re.compile(self.PLACEHOLDER_REGEX)
        self._global_data = {}

    def clear_global_data(self):
        self._global_data.clear()

    def set_global_data(self, key: str, value: any):
        """Set a global data key/value pair.

        :param key: key
        :type key: str
        :param value: value
        :type value: any
        """
        self._global_data[key] = value

    def _get_value(self, data: dict, dotted_key: str) -> any:
        """Get a value from a dict.

        :param data: dict
        :type data: dict
        :param key: key
        :type key: str
        :return: value
        :rtype: any
        """
        if not pydash.has(data, dotted_key):
            raise KeyError(f"Key {dotted_key} not found among provided params/data")
        else:
            return pydash.get(data, dotted_key)

    def _convert_match_to_variable(self, m: re.Match) -> any:
        """This is a callable, used in the _parse_string function. It will be called on
        every "value" found in the configuration dict. It converts the regex match to the
        corresponding value in the global data.

        :param m: re.Match object
        :type m: re.Match
        :return: parsed value
        :rtype: any
        """
        command, content = m.groups()
        if command.lower() == DAGSimpleParser.PLACEHOLDER_VARIABLE_NAME:
            value = self._get_value(self._global_data, content)
            return value
        else:
            return m.group()

    def _convert_match_to_item_data(self, m: re.Match, index: int, item: any) -> any:
        """This is a callable, used in the _parse_string function. It will be called on
        nodes in a foreach loop. It converts the regex match to the corresponding value/s
        of the current item.

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
        if command.lower() == DAGSimpleParser.PLACEHOLDER_ITERATION_NAME:
            value = self._get_value(local_data, content)
            return value
        else:
            return m.group()

    def _convert_match_to_argitem_data(self, m: re.Match, index: int, item: any) -> any:
        """This is a callable, used in the _parse_string function. It will be called on
        nodes in a foreach loop FOR ARGUMENTS only. It converts the regex match to the corresponding value/s
        of the current item.

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
        if command.lower() == DAGSimpleParser.PLACEHOLDER_ARG_ITERATION_NAME:
            value = self._get_value(local_data, content)
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
        empty = copy.deepcopy(node)
        flat_node = DictionaryUtils.flatten(node)
        for key, value in flat_node.items():
            flat_node[key] = self._parse_string(
                value, lambda m: self._convert_match_to_item_data(m, index, item)
            )
            pydash.set_(empty, key, flat_node[key])

        return empty

    def _parse_string(
        self, s: str, replace_cb: Callable[[re.Match], any] = _convert_match_to_variable
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
        try:
            occurrences = len(self._regex.findall(s))
        except Exception:
            occurrences = 0

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

    def _extract_foreach_data(
        self,
        node: dict,
    ) -> Tuple[Optional[dict], Optional[dict]]:
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
        'foreach data' is the data contained in the 'items' key. If no foreach data is found,
        the pseudo node and the foreach data are None.
        :rtype: Tuple[Optional[dict], Optional[dict]]
        """
        if DAGSimpleParser.PLACEHOLDER_FOREACH_NAME in node:
            foreach_node = node[DAGSimpleParser.PLACEHOLDER_FOREACH_NAME]
            pseudo_node = foreach_node.get(
                DAGSimpleParser.PLACEHOLDER_FOREACH_DO_NAME, None
            )
            foreach_data = foreach_node.get(
                DAGSimpleParser.PLACEHOLDER_FOREACH_ITEMS_NAME, None
            )
            if pseudo_node is None or foreach_data is None:
                raise KeyError("Invalid foreach node. Missing 'do' or 'items' keys.")

            return pseudo_node, foreach_data
        else:
            return None, None

    def _expand_dict_values(self, branch_cfg: dict) -> dict:
        """Parse a generic dict where values could be a foreach node. This is used to
        parse foreach(s) inside nodes arguments, like input or output ports. A branch config
        could be something like:

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

        :param branch_cfg: branch to parse
        :type branch_cfg: dict
        :raises Exception: If needed keys are missing ('do' and 'items')
        :raises Exception: If the 'do' item is not a string
        :raises Exception: If the 'items' item is not a Sequence
        :return: parsed branch
        :rtype: dict
        """

        branch_cfg = copy.deepcopy(branch_cfg)

        to_replace_data = {}
        for key, value in branch_cfg.items():
            if isinstance(value, dict):
                if DAGSimpleParser.PLACEHOLDER_FOREACH_NAME in value:
                    pseudo_node, foreach_data = self._extract_foreach_data(value)
                    if pseudo_node is not None:
                        # if not isinstance(pseudo_node, str):
                        #     raise Exception(
                        #         "Invalid foreach node. 'do' key must be a sequence."
                        #     )
                        if not isinstance(foreach_data, Sequence):
                            raise TypeError(
                                "Invalid foreach node. 'items' key must be a Sequence."
                            )
                        parsed_list = []
                        for value_index, value in enumerate(foreach_data):
                            parsed_list.append(
                                self._parse_string(
                                    pseudo_node,
                                    lambda m: self._convert_match_to_argitem_data(
                                        m, value_index, value
                                    ),
                                )
                            )
                        to_replace_data[key] = parsed_list

        for key, value in to_replace_data.items():
            branch_cfg[key] = value

        return branch_cfg

    def _replace_variables_deep(self, cfg: dict) -> dict:
        """Deep replace all variables placeholders in the config.

        :param cfg: config to parse
        :type cfg: dict
        :return: parsed config
        :rtype: dict
        """

        new_cfg = copy.deepcopy(cfg)

        # Parse variables. This is done first because the variables could be used in
        # other nodes/foreach nodes. Iterates over the whole configuration as a flat list
        # of pydash(ed) keys and values
        for key, value in DictionaryUtils.flatten(cfg).items():
            if isinstance(value, str):
                pydash.set_(
                    new_cfg,
                    key,
                    self._parse_string(
                        value, replace_cb=self._convert_match_to_variable
                    ),
                )

        return new_cfg

    def _expand_nodes(self, nodes_cfg: dict) -> dict:
        """Iterates through the nodes and expand foreach-nodes.

        :param nodes_cfg: nodes config {node_name: node_config}
        :type nodes_cfg: dict
        :return: expanded nodes config {node_name: node_config}
        :rtype: dict
        """

        nodes_cfg = copy.deepcopy(nodes_cfg)

        to_add_nodes = {}  # used to not modify original dict while iterating
        to_delete_nodes = set()  # used to not modify original dict while iterating
        for node_name in nodes_cfg:
            node = nodes_cfg[node_name]
            pseudo_node, foreach_data = self._extract_foreach_data(node)

            # if pseudo_node is not None, then the node is a foreach node!
            if pseudo_node is not None:

                if not isinstance(pseudo_node, dict):
                    raise TypeError("Invalid foreach node. 'do' key must be a dict.")

                # Iterate the foreach data and create a new node for each item
                for index, data in enumerate(foreach_data):
                    new_node_key = str(index)
                    if isinstance(data, str):
                        new_node_key = data

                    # Create a new node with a name based on original name and the index
                    to_add_nodes[f"{node_name}@{index}"] = self._parse_foreach_node(
                        pseudo_node, data, index
                    )

                # forwards the node to be deleted
                to_delete_nodes.add(node_name)

        # remove the foreach nodes
        for node_name in to_delete_nodes:
            del nodes_cfg[node_name]

        # Add the new nodes to the original config
        for node_name in to_add_nodes:
            nodes_cfg[node_name] = to_add_nodes[node_name]

        return nodes_cfg

    def _expand_nodes_arguments(self, nodes_cfg: dict) -> dict:
        """Iterate through all nodes and expand the foreach branches in arguments.

        :param nodes_cfg: nodes config {node_name: node_config}
        :type nodes_cfg: dict
        :return: expanded config {node_name: node_config}
        :rtype: dict
        """

        nodes_cfg = copy.deepcopy(nodes_cfg)

        for node_name in nodes_cfg:
            node = nodes_cfg[node_name]

            to_expand = {}
            for key, value in node.items():
                if isinstance(value, dict):
                    to_expand[key] = self._expand_dict_values(value)

            for key, value in to_expand.items():
                nodes_cfg[node_name][key] = value

        return nodes_cfg

    def _merge_multiple_arguments(self, nodes_cfg: dict) -> dict:
        """Merge node input/output/arguments with a name containing a '@' inside.
        For example:

        {
            'args': {
                'par@0': [1,2,3],
                'par@1': [4,5,6]
            }
        }

        becomes:

        {
            'args': {
                'par': [(1,2), (3,4), (5,6)]],
            }
        }

        Args:
            nodes_cfg (dict): nodes config {node_name: node_config}

        Returns:
            dict: merged config {node_name: node_config}
        """
        nodes_cfg = copy.deepcopy(nodes_cfg)

        for node_name in nodes_cfg:
            node = nodes_cfg[node_name]

            to_replace = {}
            for key, subnode in node.items():
                if isinstance(subnode, dict):
                    rephrased_args = {}
                    for arg_name, value in subnode.items():
                        if DAGSimpleParser.PLACEHOLDER_ARG_SPLIT_CHAR in arg_name:
                            arg_name, arg_index = arg_name.split(
                                DAGSimpleParser.PLACEHOLDER_ARG_SPLIT_CHAR
                            )
                            if arg_name not in rephrased_args:
                                rephrased_args[arg_name] = {}
                            rephrased_args[arg_name][arg_index] = value

                        else:
                            rephrased_args[arg_name] = value

                    for arg_name, value in list(rephrased_args.items()):
                        if isinstance(value, dict):
                            raw_values = list(value.values())
                            raw_size = len(raw_values)
                            row_size = -1
                            if raw_size > 0:
                                row_size = len(raw_values[0])

                            if row_size > 0:
                                remapped = []
                                for r in range(row_size):
                                    row = [raw_values[i][r] for i in range(raw_size)]
                                    remapped.append(row)
                                rephrased_args[arg_name] = remapped
                    to_replace[key] = rephrased_args

            for k, v in to_replace.items():
                node[k] = v

        return nodes_cfg

    def parse_cfg(self, cfg: dict, global_data: Optional[dict] = None) -> DAGModel:
        """Parse the configuration

        :param cfg: configuration to parse
        :type cfg: dict
        :param global_variables: global variables
        :type global_variables: Optional[dict], default None
        :return: nodes model
        :rtype: NodesModel
        """

        # sets global variables
        if global_data is not None:
            self.clear_global_data()
            [self.set_global_data(k, v) for k, v in global_data.items()]

        # replace all variables
        parsed = self._replace_variables_deep(cfg)

        # Parse foreach nodes. Each configuration node could contain a foreach node. This
        # means that the node generates multiple nodes based on a list of values.
        parsed[DAGConfigParser.NODES_NAMESPACE] = self._expand_nodes(
            parsed[DAGConfigParser.NODES_NAMESPACE]
        )

        # Parse the branches. AKA foreach nodes inside nodes inputs/outputs values.
        parsed[DAGConfigParser.NODES_NAMESPACE] = self._expand_nodes_arguments(
            parsed[DAGConfigParser.NODES_NAMESPACE]
        )

        # Merge multiple arguments as tuples
        parsed[DAGConfigParser.NODES_NAMESPACE] = self._merge_multiple_arguments(
            parsed[DAGConfigParser.NODES_NAMESPACE]
        )

        return DAGModel(**parsed)
