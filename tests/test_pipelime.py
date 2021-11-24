#!/usr/bin/env python

"""Tests for `pipelime` package."""

import pytest
from click.testing import CliRunner

from pipelime import cli, pipelime


@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """
    # import requests
    # return requests.get('https://github.com/audreyr/cookiecutter-pypackage')


def test_content(response):
    """Sample pytest test function with the pytest fixture as an argument."""
    # from bs4 import BeautifulSoup
    # assert 'GitHub' in BeautifulSoup(response.content).title.string


# def test_command_line_interface():
#     """Test the CLI."""
#     runner = CliRunner()
#     result = runner.invoke(cli.main)
#     assert result.exit_code == 0
#     assert 'pipelime.cli.main' in result.output
#     help_result = runner.invoke(cli.main, ['--help'])
#     assert help_result.exit_code == 0
#     assert '--help  Show this message and exit.' in help_result.output


def test_toy_dataset_small(toy_dataset_small):

    for x in [
        "folder",
        "data_folder",
        "size",
        "image_size",
        "zfill",
        "expected_keys",
        "keypoints_format",
    ]:
        assert x in toy_dataset_small
