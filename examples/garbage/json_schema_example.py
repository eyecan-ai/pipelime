from pipelime.sequences.samples import Sample
import rich
from schema import Schema
from pipelime.sequences.readers.filesystem import UnderfolderReader


class SchemaLoader(object):
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
        import uuid
        # import importlib.util
        # spec = importlib.util.spec_from_file_location(str(uuid.uuid1()), path)
        # mod = importlib.util.module_from_spec(spec)
        # spec.loader.exec_module(mod)

        import types
        import importlib.machinery
        loader = importlib.machinery.SourceFileLoader(str(uuid.uuid1()), path)
        mod = types.ModuleType(loader.name)
        loader.exec_module(mod)
        return mod


s = SchemaLoader().load(filename='_schema.schema')


folder = '/Users/daniele/Downloads/lego_dataset/lego_00'
reader = UnderfolderReader(folder=folder)

for sample in reader:
    sample: Sample
    print(sample.id)

    sample.validate(s, deep=False)
