from schema import Schema
import uuid
import types
import importlib.machinery


class SchemaLoader:
    DEFAULT_SCHEMA_VARIABLE_NAME = 'schema'

    @classmethod
    def load(cls, filename: str) -> Schema:
        module = cls._load_module(filename)
        sname = SchemaLoader.DEFAULT_SCHEMA_VARIABLE_NAME
        if not hasattr(module, sname):
            raise ModuleNotFoundError(f'No variable with name {sname} found in file: {filename}')
        return Schema(schema=getattr(module, sname))

    @classmethod
    def _load_module(cls, path: str):
        loader = importlib.machinery.SourceFileLoader(str(uuid.uuid1()), path)
        mod = types.ModuleType(loader.name)
        loader.exec_module(mod)
        return mod
