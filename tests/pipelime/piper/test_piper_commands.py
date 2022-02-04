import pytest
from pipelime.pipes.piper import Piper
import rich


class TestCustomPiperCommands:
    def _build_command_string(self, python_command: str):
        # import platform

        # if platform.system() == "Windows":
        #     return f"python.exe {python_command}"
        # else:
        return f"python {python_command}"

    def test_custom_commands(self, piper_commands: dict):

        for command_name, item in piper_commands.items():
            filename = item["filename"]
            valid = item["valid"]
            rich.print("Testing\n", item, valid)

            if valid:
                description = Piper.piper_command_description(
                    self._build_command_string(str(filename))
                )
                assert isinstance(description, dict)

            else:
                exc = item["exception"]
                with pytest.raises(exc):
                    description = Piper.piper_command_description(
                        self._build_command_string(str(filename))
                    )
