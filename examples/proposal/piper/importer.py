from importlib.machinery import SourceFileLoader
from inspect import getmembers, getsource, isfunction
from click.core import Command, Option
import rich

from pipelime.pipes.piper import Piper


info = Piper.piper_command_description("python alpha.py")
rich.print(info)

# foo = SourceFileLoader("", "alpha.py").load_module()
# for name, func in getmembers(foo, lambda x: isinstance(x, Command)):
#     print(name, func)
#     command: Command = func
#     for option in command.params:
#         option: Option
#         print(option.name, option.type, option.opts)
#         print(option.default)
