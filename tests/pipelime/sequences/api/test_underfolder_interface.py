import io
import shutil
import rich
import pytest
from pipelime.sequences.api.base import EntitySample, EntitySampleData
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.api.underfolder import UnderfolderInterface


class TestUnderfolderInterface:
    def test_interface_interaction(self, sample_underfolder_minimnist, tmp_path):

        # creates a copy of minimnist dataset under a new folder
        folder = tmp_path / "dataset"
        shutil.copytree(sample_underfolder_minimnist["folder"], folder)
        print("folder:", folder)

        # base reader
        dataset = UnderfolderReader(folder=folder)

        # corresponding interface
        interface = UnderfolderInterface("inter", folder, allowed_keys=None)

        assert len(dataset) > 0
        ref_sample = dataset[0]
        keys = list(ref_sample.keys())

        # retrieve dataset entity
        dataset_entity = interface.get_dataset()

        # assert manifest consistency
        assert len(dataset) == dataset_entity.manifest.size
        assert set(keys) == set(dataset_entity.manifest.keys)
        assert len(dataset_entity.manifest.sample_ids) == len(dataset)

        # store metadata keys
        meta_keys = []

        # match ids
        for sample in dataset:
            assert sample.id in dataset_entity.manifest.sample_ids

            sample_entity = interface.get_sample(sample.id)
            assert sample_entity is not None

            assert sample_entity.id == sample.id

            for key in keys:
                assert key in sample_entity.metadata or key in sample_entity.data

                if key in sample_entity.data:
                    sample_data_entity = sample_entity.data[key]
                    assert isinstance(sample_data_entity, EntitySampleData)

                    data, mimetype = interface.get_sample_data(
                        sample_id=sample.id, item_name=key
                    )
                    assert isinstance(data, io.BytesIO)

                if key in sample_entity.metadata:
                    meta_keys.append(key)
                    sample_entity.metadata[key] = {"overwrite": True}

                    interface.put_sample(sample.id, sample_entity)

        new_dataset = UnderfolderReader(folder=folder)
        for sample in new_dataset:
            for key in meta_keys:
                assert key in sample
                assert "overwrite" in sample[key]
                assert sample[key]["overwrite"]

    @pytest.mark.parametrize(
        "allowed_item",
        [
            {
                "allowed_keys": None,
                "to_write": ["metadata", "metadatay"],
                "allowed": True,
            },
            {
                "allowed_keys": ["metadata"],
                "to_write": ["metadata", "metadatay"],
                "allowed": False,
            },
            {
                "allowed_keys": ["metadatay"],
                "to_write": ["metadata", "metadatay"],
                "allowed": False,
            },
            {
                "allowed_keys": ["metadata"],
                "to_write": ["metadata"],
                "allowed": True,
            },
            {
                "allowed_keys": ["metadatay"],
                "to_write": ["metadatay"],
                "allowed": True,
            },
        ],
    )
    def test_keys_protection(
        self,
        sample_underfolder_minimnist,
        tmp_path,
        allowed_item,
    ):

        allowed_keys = allowed_item["allowed_keys"]
        to_write = allowed_item["to_write"]
        allowed = allowed_item["allowed"]

        # creates a copy of minimnist dataset under a new folder
        folder = tmp_path / "dataset"
        shutil.copytree(sample_underfolder_minimnist["folder"], folder)
        print("folder:", folder)

        # base reader
        dataset = UnderfolderReader(folder=folder)

        # corresponding interface
        interface = UnderfolderInterface("inter", folder, allowed_keys=allowed_keys)

        assert len(dataset) > 0
        ref_entity = interface.get_sample(dataset[0].id)

        metadata = {}
        for key in to_write:
            metadata[key] = {"overwrite": True}

        sample_entity = EntitySample(id=ref_entity.id, data={}, metadata=metadata)

        rich.print(allowed, "metadata", metadata, "allowed", allowed_keys)
        if allowed:
            interface.put_sample(ref_entity.id, sample_entity)
        else:
            with pytest.raises(PermissionError):
                interface.put_sample(ref_entity.id, sample_entity)
