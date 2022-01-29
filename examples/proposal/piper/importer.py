import rich
from pipelime.pipes.piper import Piper

info = Piper.piper_command_description("python alpha.py")
rich.print(info)
