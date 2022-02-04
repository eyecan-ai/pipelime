import dictquery
from typing import Any, List, Sequence, Tuple, Dict, Optional


class DictionaryWalker:
    @classmethod
    def flatten_as_lists(
        cls,
        d: dict,
        discard_keys: Optional[Sequence[str]] = None,
    ) -> Sequence[Tuple[List[str], Any]]:
        return cls.walk(d, discard_keys=discard_keys)

    @classmethod
    def flatten_as_tuples(
        cls,
        d: dict,
        discard_keys: Optional[Sequence[str]] = None,
    ) -> Sequence[Tuple[Tuple[str], Any]]:
        return [(tuple(x), y) for x, y in cls.walk(d, discard_keys=discard_keys)]

    @classmethod
    def flatten_as_dicts(
        cls,
        d: dict,
        discard_keys: Optional[Sequence[str]] = None,
    ) -> Sequence[Tuple[str, Any]]:

        return [
            (".".join(x), y)
            for x, y in cls.flatten_as_lists(d, discard_keys=discard_keys)
        ]

    @classmethod
    def flatten(
        cls,
        d: dict,
        discard_keys: Optional[Sequence[str]] = None,
    ) -> dict:

        return {k: v for k, v in cls.flatten_as_dicts(d, discard_keys=discard_keys)}

    @classmethod
    def walk(
        cls,
        d: Dict,
        path: Sequence = None,
        chunks: Sequence = None,
        discard_keys: Optional[Sequence[str]] = None,
    ) -> Sequence[Tuple[str, Any]]:
        """Deep visit of dictionary building a plain sequence of pairs(key, value) where key has a pydash notation
        : param d: input dictionary
        : type d: Dict
        : param path: private output value for path(not use), defaults to None
        : type path: Sequence, optional
        : param chunks: private output to be fileld with retrieved pairs(not use), defaults to None
        : type chunks: Sequence, optional
        : param discard_private_qualifiers: TRUE to discard keys starting with private qualifier, defaults to True
        : type discard_private_qualifiers: bool, optional
        : return: sequence of retrieved pairs
        : rtype: Sequence[Tuple[str, Any]]
        """

        discard_keys = discard_keys if discard_keys is not None else []
        root = False
        if path is None:
            path, chunks, root = [], [], True
        if isinstance(d, dict):
            for k, v in d.items():
                path.append(k)
                if isinstance(v, dict) or isinstance(v, list):
                    cls.walk(
                        v,
                        path=path,
                        chunks=chunks,
                        discard_keys=discard_keys,
                    )
                else:
                    keys = list(map(str, path))
                    if not (
                        len(discard_keys) > 0
                        and any([x for x in discard_keys if x in keys])
                    ):
                        chunks.append((keys, v))
                path.pop()
        elif isinstance(d, list):
            for idx, v in enumerate(d):
                path.append(idx)
                cls.walk(
                    v,
                    path=path,
                    chunks=chunks,
                    discard_keys=discard_keys,
                )
                path.pop()
        else:
            keys = list(map(str, path))
            chunks.append((keys, d))
        if root:
            return chunks


class DictionaryUtils:
    @classmethod
    def flatten(cls, d: dict, parent_key="", sep=".") -> Dict[str, any]:
        """Computes a flattened dictionary.
        For example:

        {
            "a": {
                "b": {
                    "c": 1
                }
                "d": 2
            }
        }

        becomes:

        {
            "a.b.c": 1,
            "a.d": 2
        }

        :param d: [description]
        :type d: dict
        :param parent_key: [description], defaults to ""
        :type parent_key: str, optional
        :param sep: [description], defaults to "."
        :type sep: str, optional
        :return: [description]
        :rtype: Dict[str, any]
        """
        return {k: v for k, v in DictionaryWalker.flatten_as_dicts(d)}


class DictSearch:
    KEY_PLACEHOLDER = "$V"

    @classmethod
    def match_queries(cls, proto_dict: dict, target_dict: dict) -> bool:
        """Match a dict against a dict proto. The dict proto is a dict where values
        are 'dictquery'-like strings (i.e. info here:
        https://github.com/cyberlis/dictquery).
        For example with the proto dict:

        .. code-block:: python

            {
                'a': {
                    'b': {
                        'c': '>= 1',
                    },
                },
            }

        will match positive if the target dict is {'a': {'b': {'c': 10}}} and negative
        if the target dict is {'a': {'b': {'c': 0}}}.

        The proto dict can contains also multiple occurrences of the same key. In this
        case the value should contains placeholders for the occurrence number.
        For example:

        .. code-block:: python

            {
                'a': {
                    'b': '$V >= 2 AND $V <= 10'
            }

        The proto dict can contains also None values, in this case nothing happens, the
        query is bypassed.

        :param proto_dict: the proto dict
        :type proto_dict: dict
        :param target_dict: the target dict to match
        :type target_dict: dict
        :return: TRUE if query matches, FALSE otherwise
        :rtype: bool
        """

        flatten_proto_dict = DictionaryUtils.flatten(proto_dict)

        valid = True
        for key, value in flatten_proto_dict.items():
            query = DictSearch.build_query(key, value)
            if query is not None:
                valid = dictquery.match(target_dict, query)
            if not valid:
                break
        return valid

    @classmethod
    def build_query(cls, key: str, value: Optional[str] = None) -> Optional[str]:
        if value is None:
            return None
        if "$V" not in value:
            return f"`{key}` {value}"
        else:
            return value.replace("$V", f"`{key}`")
