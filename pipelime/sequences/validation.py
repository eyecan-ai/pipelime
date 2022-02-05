import importlib.machinery
import types
import uuid

from schema import Schema, SchemaError

from pipelime.sequences.samples import Sample
from pipelime.sequences.stages import SampleStage
from pipelime.sequences.operations import OperationStage


class SampleSchema:
    class ValidationError(Exception):
        pass

    def __init__(self, schema: Schema, deep: bool = True):
        self._schema = schema
        self._deep = deep

    @property
    def deep(self) -> bool:
        return self._deep

    @property
    def schema(self) -> Schema:
        return self._schema

    def validate(self, sample: Sample):
        try:
            if not self.deep:
                self.schema.validate(sample.skeleton)
            else:
                self.schema.validate(dict(sample))
        except SchemaError as e:
            raise SampleSchema.ValidationError(e)


class SchemaLoader:
    DEFAULT_SCHEMA_VARIABLE_NAME = "schema"
    DEFAULT_DEEP_VARIABLE_NAME = "deep"

    @classmethod
    def load(cls, filename: str) -> SampleSchema:
        module = cls._load_module(str(filename))
        sname = SchemaLoader.DEFAULT_SCHEMA_VARIABLE_NAME
        if not hasattr(module, sname):
            raise ModuleNotFoundError(
                f"No variable with name {sname} found in file: {filename}"
            )

        schema = Schema(getattr(module, sname))
        deep = getattr(module, SchemaLoader.DEFAULT_DEEP_VARIABLE_NAME, True)
        return SampleSchema(schema=schema, deep=deep)

    @classmethod
    def _load_module(cls, path: str):
        loader = importlib.machinery.SourceFileLoader(str(uuid.uuid1()), path)
        mod = types.ModuleType(loader.name)
        loader.exec_module(mod)
        return mod


class StageValidate(SampleStage):
    def __init__(self, sample_schema: SampleSchema):
        self._sample_schema = sample_schema

    def __call__(self, x: Sample) -> Sample:
        try:
            self._sample_schema.validate(x)
        except Exception as e:
            raise SampleSchema.ValidationError(f"Sample ID: {x.id} -> {str(e)}")
        return x


class OperationValidate(OperationStage):
    def __init__(self, sample_schema: SampleSchema, **kwargs):
        super().__init__(stage=StageValidate(sample_schema), **kwargs)
