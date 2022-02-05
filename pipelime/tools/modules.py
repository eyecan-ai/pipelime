import importlib
import types
import uuid


class ModulesUtils:
    @classmethod
    def load_module_from_file(cls, path: str) -> types.ModuleType:
        loader = importlib.machinery.SourceFileLoader(str(uuid.uuid1()), path)
        mod = types.ModuleType(loader.name)
        loader.exec_module(mod)
        return mod

    @classmethod
    def load_variable_from_file(cls, path: str, name: str) -> any:
        module = ModulesUtils.load_module_from_file(path)
        if not hasattr(module, name):
            raise ModuleNotFoundError(
                f"No variable with name {name} found in file: {path}"
            )
        return getattr(module, name)
