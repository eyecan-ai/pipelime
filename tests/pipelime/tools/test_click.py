from pipelime.tools.click import ClickTools
from types import SimpleNamespace
from deepdiff import DeepDiff
import pytest


class TestClickTools:
    @pytest.mark.parametrize(
        "args, expected",
        [
            (
                [
                    "-i",
                    "dag.yml",
                    "-p",
                    "params.yml",
                    "-o",
                    "output.yml",
                    "--number",
                    "2",
                    "--boolean",
                    "False",
                ],
                {
                    "i": "dag.yml",
                    "p": "params.yml",
                    "o": "output.yml",
                    "number": "2",
                    "boolean": "False",
                },
            ),
            (
                [],
                {},
            ),
        ],
    )
    def test_additional_arguments(self, args, expected):

        # monkey patch
        ctx = SimpleNamespace(**{"args": []})
        ctx.args = args
        result = ClickTools.parse_additional_args(ctx)
        assert not DeepDiff(expected, result)
