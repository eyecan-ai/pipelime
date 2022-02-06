import pytest
from pipelime.pipes.piper import Piper
import rich


class TestCustomPiperCommands:
    def test_custom_commands(self, piper_commands: dict):

        assert len(piper_commands.items()) > 0  # PARANOID!

        for command_name, item in piper_commands.items():
            command = item["command"]
            valid = item["valid"]
            rich.print("Testing\n", item, valid)

            if valid:
                description = Piper.piper_command_description(command)
                assert isinstance(description, dict)

            else:
                exc = item["exception"]
                with pytest.raises(exc):
                    description = Piper.piper_command_description(command)
