import pytest
from pathlib import Path


@pytest.fixture(scope="session")
def piper_commands(data_folder):
    return {
        "fake_detector": {
            "filename": Path(data_folder)
            / "piper"
            / "piper_commands"
            / "fake_detector.py",
            "valid": True,
            "exception": None,
        },
        "wrong_outputs": {
            "filename": Path(data_folder)
            / "piper"
            / "piper_commands"
            / "wrong_outputs.py",
            "valid": False,
            "exception": KeyError,
        },
        "wrong_inputs": {
            "filename": Path(data_folder)
            / "piper"
            / "piper_commands"
            / "wrong_inputs.py",
            "valid": False,
            "exception": KeyError,
        },
        "not_a_piper": {
            "filename": Path(data_folder)
            / "piper"
            / "piper_commands"
            / "not_a_piper.py",
            "valid": False,
            "exception": TypeError,
        },
        "no_command_name": {
            "filename": Path(data_folder)
            / "piper"
            / "piper_commands"
            / "no_command_name.py",
            "valid": True,
            "exception": None,
        },
    }
