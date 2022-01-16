import collections
from typing import Dict, Optional
import dictquery
from choixe.configurations import XConfig


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
        return {
            k: v
            for k, v in XConfig.from_dict(d).chunks(discard_private_qualifiers=True)
        }


class DictSearch:
    KEY_PLACEHOLDER = "$V"

    @classmethod
    def match_queries(cls, proto_dict: dict, target_dict: dict) -> bool:
        """Match a dict against a dict proto. The dict proto is a dict where values
        are 'dictquery'-like strings (i.e. info here: https://github.com/cyberlis/dictquery).
        For example with the proto dict:

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
        case the value should contains placeholders for the occurrence number. For example:

        {
            'a': {
                'b': '$V >= 2 AND $V <= 10'
        }

        The proto dict can contains also None values, in this case nothing happens, the query
        is bypassed.


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
    def build_query(cls, key: str, value: Optional[str] = None) -> str:
        if value is None:
            return None
        if "$V" not in value:
            return f"`{key}` {value}"
        else:
            return value.replace("$V", f"`{key}`")
