import collections
import dictquery


class DictionaryUtils:
    @classmethod
    def flatten(cls, d, parent_key="", sep="."):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(cls.flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)


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
            valid = dictquery.match(target_dict, query)
            if not valid:
                break
        return valid

    @classmethod
    def build_query(cls, key: str, value: str) -> str:
        if "$V" not in value:
            return f"`{key}` {value}"
        else:
            return value.replace("$V", f"`{key}`")