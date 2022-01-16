import io
import shutil
import rich
import pytest
from pipelime.sequences.api.entities import EntitySample, EntitySampleData
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.api.underfolder import UnderfolderInterface
from pipelime.tools.dictionaries import DictSearch


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

    # TEST SEARCH SAMPLES
    @pytest.mark.parametrize(
        "search_item",
        [
            {"metadata": {"info": {}}, "expected": 20},
            {"metadata": {"info": {"sample_id": "== 0"}}, "expected": 1},
            {"metadata": {"info": {"sample_id": "> 0"}}, "expected": 19},
            {"metadata": {"info": {"sample_id": ">= 0"}}, "expected": 20},
            {"metadata": {"info": {"double": "<= 20"}}, "expected": 11},
            {
                "metadata": {"info": {"double": "<= 20", "sample_id": "< 6"}},
                "expected": 6,
            },
            {
                "metadata": {
                    "info": {"double": "<= 20", "sample_id": "< 6", "half": "== 0"}
                },
                "expected": 1,
            },
            {
                "metadata": {"categories": {"main": "== 'category_odd'"}},
                "expected": 10,
            },
            {
                "metadata": {"categories": {"main": "!= 'category_odd'"}},
                "expected": 10,
            },
            {
                "metadata": {"categories": {"main": "LIKE 'category*'"}},
                "expected": 20,
            },
            {
                "metadata": {"categories": {"main": "LIKE '*odd'"}},
                "expected": 10,
            },
            {
                "metadata": {
                    "info": {"sample_id": "< 5"},
                    "categories": {"main": "LIKE '*odd'"},
                },
                "expected": 2,
            },
            {
                "metadata": {
                    "categories": {"others": "CONTAINS 'alpha'"},
                },
                "expected": 10,
            },
            {
                "metadata": {
                    "info": None,
                    "categories": {"others": "CONTAINS 'alpha'"},
                },
                "expected": 10,
            },
            {
                "metadata": {
                    "categories": {
                        "others": f"{DictSearch.KEY_PLACEHOLDER} CONTAINS 'alpha' and {DictSearch.KEY_PLACEHOLDER} CONTAINS 'beta'"
                    },
                },
                "expected": 10,
            },
            {
                "metadata": {
                    "categories": {
                        "others": f"{DictSearch.KEY_PLACEHOLDER} CONTAINS 'alpha' and {DictSearch.KEY_PLACEHOLDER} CONTAINS 'zeta'"
                    },
                },
                "expected": 0,
            },
            {
                "metadata": {
                    "categories": {
                        "others": f"{DictSearch.KEY_PLACEHOLDER} CONTAINS 'alpha' OR {DictSearch.KEY_PLACEHOLDER} CONTAINS 'zeta'"
                    },
                },
                "expected": 20,
            },
            {
                "metadata": {
                    "info": {
                        "sample_id": f"{DictSearch.KEY_PLACEHOLDER} >= 1 AND {DictSearch.KEY_PLACEHOLDER} <= 2"
                    },
                },
                "expected": 2,
            },
        ],
    )
    def test_search_samples(
        self, sample_underfolder_minimnist_queries, tmp_path, search_item
    ):

        # creates underfolders from minimnist sample data
        dataset_name = "searchable"
        folder = str(tmp_path / dataset_name)
        shutil.copytree(sample_underfolder_minimnist_queries["folder"], folder)

        # creates the api
        interface = UnderfolderInterface(dataset_name, folder)

        # Search ALL
        metadata = search_item["metadata"]
        expected_count = search_item["expected"]

        rich.print("Search item", search_item)
        samples_entities = interface.search_samples(
            EntitySample(id=-1, metadata=metadata, data={})
        )

        assert len(samples_entities) == expected_count
