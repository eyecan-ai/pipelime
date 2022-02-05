from abc import ABC, abstractclassmethod, abstractmethod
import importlib.machinery
from pathlib import Path
import types
import uuid
from schema import Schema, SchemaError
from pipelime.sequences.samples import Sample
from pipelime.sequences.stages import SampleStage
from pipelime.sequences.operations import OperationStage


class SampleSchema(ABC):
    class ValidationError(Exception):
        pass

    @abstractmethod
    def validate(self, sample: Sample) -> bool:
        """Validate a sample.

        Args:
            sample (Sample): The sample to validate.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            bool: True if the sample is valid, False otherwise.
        """
        raise NotImplementedError

    @classmethod
    @abstractclassmethod
    def load_from_file(cls, filename: str) -> "SampleSchema":
        """Load a schema from a file.

        Raises:
            NotImplementedError: If the method is not implemented.
        """
        raise NotImplementedError


class SampleSchemaWithSchema(SampleSchema):
    DEFAULT_SCHEMA_VARIABLE_NAME = "schema"
    DEFAULT_DEEP_VARIABLE_NAME = "deep"

    def __init__(self, schema: Schema, deep: bool = True):
        self._schema = schema
        self._deep = deep

    @property
    def deep(self) -> bool:
        return self._deep

    @property
    def schema(self) -> Schema:
        return self._schema

    def validate(self, sample: Sample) -> bool:
        """Validate a sample with 'schema'.

        Args:
            sample (Sample): The sample to validate.

        Raises:
            SampleSchema.ValidationError: If the sample is not valid.

        Returns:
            bool: True if the sample is valid, False otherwise.
        """
        try:
            if not self.deep:
                self.schema.validate(sample.skeleton)
            else:
                self.schema.validate(dict(sample))
        except SchemaError as e:
            raise SampleSchema.ValidationError(e)
        return True

    @classmethod
    def _load_module(cls, path: str):
        loader = importlib.machinery.SourceFileLoader(str(uuid.uuid1()), path)
        mod = types.ModuleType(loader.name)
        loader.exec_module(mod)
        return mod

    @classmethod
    def load_from_file(cls, filename: str) -> "SampleSchemaWithSchema":
        """Load a schema from a file parsing it as a python file. The python file
        should contain a DEFAULT_SCHEMA_VARIABLE_NAME and a DEFAULT_DEEP_VARIABLE_NAME

        Raises:
            ModuleNotFoundError: [description]

        Returns:
            [type]: [description]
        """
        module = cls._load_module(str(filename))
        sname = cls.DEFAULT_SCHEMA_VARIABLE_NAME
        if not hasattr(module, sname):
            raise ModuleNotFoundError(
                f"No variable with name {sname} found in file: {filename}"
            )

        schema = Schema(getattr(module, sname))
        deep = getattr(module, cls.DEFAULT_DEEP_VARIABLE_NAME, True)
        return SampleSchemaWithSchema(schema=schema, deep=deep)


class SchemaLoader:
    SCHEMA_EXTENSIONS_MAP = {"schema": SampleSchemaWithSchema}

    @classmethod
    def is_a_valid_schema_file(cls, filename: str) -> bool:
        """Check if a file is a valid schema file.

        Args:
            filename (str): The file to check.

        Returns:
            bool: True if the file is a valid schema file, False otherwise.
        """
        return Path(filename).suffix.replace(".", "") in cls.SCHEMA_EXTENSIONS_MAP

    @classmethod
    def load(cls, filename: str) -> SampleSchema:
        """Loads a generic schema from a file. This is a factory method based on the
        file extension.

        Args:
            filename (str): The file to load the schema from.

        Raises:
            NotImplementedError: If the file extension is not supported.

        Returns:
            SampleSchema: The loaded SampleSchema.
        """
        ext = Path(filename).suffix.replace(".", "")
        if not cls.is_a_valid_schema_file(filename):
            raise NotImplementedError(
                f"File extension {ext} is not supported. Supported extensions"
                f"are: {cls.SCHEMA_EXTENSIONS_MAP.keys()}"
            )

        return cls.SCHEMA_EXTENSIONS_MAP[ext].load_from_file(filename)


class StageValidate(SampleStage):
    """This is a generic stage that validates a sample. Accepts a generic schema"""

    def __init__(self, sample_schema: SampleSchema):
        self._sample_schema = sample_schema

    def __call__(self, x: Sample) -> Sample:
        try:
            self._sample_schema.validate(x)
        except Exception as e:
            raise SampleSchema.ValidationError(f"Sample ID: {x.id} -> {str(e)}")
        return x


class OperationValidate(OperationStage):
    """This is an OperationStage that validates a sample sequence. Accepts a generic schema"""

    def __init__(self, sample_schema: SampleSchema, **kwargs):
        super().__init__(stage=StageValidate(sample_schema), **kwargs)
