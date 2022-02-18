import pytest
import rich
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.validation import (
    SampleSchema,
    SchemaLoader,
    StageValidate,
    OperationValidate,
)


class TestSampleSchemaValidation(object):
    def test_samples_validation(self, filesystem_datasets, tmp_path_factory):

        dataset_folder = filesystem_datasets["minimnist_underfolder"]["folder"]
        schemas = filesystem_datasets["minimnist_underfolder"]["schemas"]

        reader = UnderfolderReader(folder=dataset_folder)

        for schema_name, schema in schemas.items():
            rich.print("Validation", schema_name, schema)
            filename = schema["filename"]
            valid = schema["valid"]
            should_pass = schema["should_pass"]

            if valid:
                schema = SchemaLoader.load(filename)
            else:
                with pytest.raises(ModuleNotFoundError):
                    SchemaLoader.load(filename)
                continue

            for sample in reader:
                if should_pass:
                    schema.validate(sample)
                else:
                    with pytest.raises(SampleSchema.ValidationError):
                        schema.validate(sample)

    def test_samples_validation_with_stage(self, filesystem_datasets, tmp_path_factory):

        dataset_folder = filesystem_datasets["minimnist_underfolder"]["folder"]
        schemas = filesystem_datasets["minimnist_underfolder"]["schemas"]

        reader = UnderfolderReader(folder=dataset_folder)

        for schema_name, schema in schemas.items():
            rich.print("Validation", schema_name, schema)
            filename = schema["filename"]
            valid = schema["valid"]
            should_pass = schema["should_pass"]

            if valid:
                schema = SchemaLoader.load(filename)
            else:
                with pytest.raises(ModuleNotFoundError):
                    SchemaLoader.load(filename)
                continue

            for sample in reader:
                if should_pass:
                    StageValidate(schema)(sample)
                else:
                    with pytest.raises(SampleSchema.ValidationError):
                        StageValidate(schema)(sample)

    def test_samples_validation_with_operation(
        self, filesystem_datasets, tmp_path_factory
    ):

        dataset_folder = filesystem_datasets["minimnist_underfolder"]["folder"]
        schemas = filesystem_datasets["minimnist_underfolder"]["schemas"]

        reader = UnderfolderReader(folder=dataset_folder)

        for schema_name, schema in schemas.items():
            filename = schema["filename"]
            valid = schema["valid"]
            should_pass = schema["should_pass"]

            if valid:
                schema = SchemaLoader.load(filename)
            else:
                with pytest.raises(ModuleNotFoundError):
                    SchemaLoader.load(filename)
                continue

            op = OperationValidate(sample_schema=schema)
            if should_pass:
                op(reader)
            else:
                with pytest.raises(SampleSchema.ValidationError):
                    op(reader)
