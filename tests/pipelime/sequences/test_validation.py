

from schema import SchemaError
from pipelime.sequences.validation import SchemaLoader
from pipelime.sequences.readers.filesystem import UnderfolderReader
import pytest


class TestSampleSchemaValidation(object):

    def test_samples_validation(self, filesystem_datasets, tmp_path_factory):

        dataset_folder = filesystem_datasets['minimnist_underfolder']['folder']
        schemas = filesystem_datasets['minimnist_underfolder']['schemas']

        reader = UnderfolderReader(folder=dataset_folder)

        for schema_name, schema in schemas.items():
            filename = schema['filename']
            valid = schema['valid']
            should_pass = schema['should_pass']

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
                    with pytest.raises(SchemaError):
                        schema.validate(sample)
