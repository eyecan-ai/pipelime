import io
import shutil
from fastapi.exceptions import HTTPException

import imageio
from pipelime.sequences.api.entities import (
    EntityDataset,
    EntityPagination,
    EntitySample,
    EntitySampleSearchRequest,
    EntitySampleSearchResponse,
)
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.api.underfolder import UnderfolderAPI, UnderfolderInterface
from fastapi.testclient import TestClient
import pytest
import rich

from pipelime.tools.dictionaries import DictSearch


class TestUnderfolderAPIBasic:
    def test_api_basic(self, sample_underfolder_minimnist, tmp_path):

        # creates underfolders from minimnist sample data
        folders_names = ["A", "B"]
        folders_map = {}
        readers_map = {}
        keys_map = {}
        for folder_name in folders_names:
            folder_path = tmp_path / folder_name
            shutil.copytree(sample_underfolder_minimnist["folder"], folder_path)
            folders_map[folder_name] = str(folder_path)
            readers_map[folder_name] = UnderfolderReader(folder_path)
            assert len(readers_map[folder_name]) > 0
            keys_map[folder_name] = set(readers_map[folder_name][0].keys())

        # creates the api
        api = UnderfolderAPI(underfolders_map=folders_map, auth_callback=None)

        # creates the API client
        client = TestClient(api)
        assert client is not None

        response = client.get("/datasets")
        assert response.status_code == 200

        # Iterate to retrieved datasets
        for dataset_name in response.json():

            # Assert the dataset is an Entity
            rich.print("Dataset", dataset_name)
            assert dataset_name in folders_names
            dataset_entity = EntityDataset(**response.json()[dataset_name])
            assert isinstance(dataset_entity, EntityDataset)

            # Get the wrong dataset
            with pytest.raises(HTTPException):
                client.get("/dataset/IMPOSSIBLE_NAME!@/")

            # Get the dataset Entity from the single get_dataset endpoint
            get_dataset_response = client.get(f"/dataset/{dataset_name}")
            assert response.status_code == 200
            dataset_entity2 = EntityDataset(**get_dataset_response.json())
            assert isinstance(dataset_entity2, EntityDataset)

            # Checks the dataset Entity names are the same
            assert dataset_entity2.name == dataset_name
            assert dataset_entity2.name == dataset_name

            # checks for size stored in metadata is the same as the corresponding reader
            assert dataset_entity.manifest.size == len(readers_map[dataset_name])
            assert set(dataset_entity.manifest.keys) == keys_map[dataset_name]
            assert len(dataset_entity.manifest.sample_ids) == len(
                readers_map[dataset_name]
            )

            for sample_id in dataset_entity.manifest.sample_ids:

                # wrong dataset name
                with pytest.raises(HTTPException):
                    client.get(f"/dataset/IMPOSSIBLE_DATAS3T_nMAE!/{sample_id}")

                # wrong sample id
                with pytest.raises(HTTPException):
                    client.get(f"/dataset/{dataset_name}/-1")

                # Get sample response
                get_sample_response = client.get(f"/dataset/{dataset_name}/{sample_id}")
                assert get_sample_response.status_code == 200

                # Get sample entity from response
                sample_entity = EntitySample(**get_sample_response.json())
                assert isinstance(sample_entity, EntitySample)

                # Check sample entity id is the same as the sample id
                assert sample_entity.id == sample_id

                # Check sample entity metadata has key as raw dataset
                for key in sample_entity.metadata:
                    assert key in keys_map[dataset_name]
                    assert sample_entity.metadata[key] is not None

                # Check sample entity data has key as raw dataset
                for key in sample_entity.data:
                    assert key in keys_map[dataset_name]
                    assert sample_entity.data[key] is not None

                    # For each raw data fetch corresponding file binaray data
                    rich.print("Request:", f"/dataset/{dataset_name}/{sample_id}/{key}")
                    get_sample_data_request = client.get(
                        f"/dataset/{dataset_name}/{sample_id}/{key}"
                    )

                    # TEMPORARY TEST FOR FORMAT CHECKS
                    with pytest.raises(NotImplementedError):
                        client.get(
                            f"/dataset/{dataset_name}/{sample_id}/{key}?format=WOW"
                        )

                    # wrong dataset name
                    with pytest.raises(HTTPException):
                        client.get(
                            f"/dataset/IMPOSSIBLE_DATSAET_NMAE!/{sample_id}/{key}"
                        )

                    # wrong sample id
                    with pytest.raises(HTTPException):
                        client.get(f"/dataset/{dataset_name}/-1/{key}")

                    # wrong item name
                    with pytest.raises(HTTPException):
                        client.get(
                            f"/dataset/{dataset_name}/{sample_id}/IMPOSSIBLE_NAME"
                        )

                    # Checks the response is a binary data and is oK
                    assert get_sample_data_request.status_code == 200
                    assert len(get_sample_data_request.content) > 0

                    # If the data is an image, check the image is readable
                    if sample_entity.data[key].type == "image":
                        img = imageio.imread(
                            io.BytesIO(get_sample_data_request.content)
                        )
                        assert img is not None

                # Pick random metadata and modify it
                assert "metadata" in sample_entity.metadata
                sample_entity.metadata["metadata"] = {"put": "inception"}
                sample_entity.metadata["metadatay"] = [1, 2, 3, 4, 5]
                sample_entity.data = {}

                # wrong dataset name
                with pytest.raises(HTTPException):
                    client.put(
                        f"/dataset/IMPOSSIBLE_DATAS3T_nMAE!/{sample_id}",
                        json=sample_entity.dict(),
                    )

                # wrong sample id
                with pytest.raises(HTTPException):
                    client.put(f"/dataset/{dataset_name}/-1", json=sample_entity.dict())

                # Update the sample with changed metadata
                put_sample_request = client.put(
                    f"/dataset/{dataset_name}/{sample_id}", json=sample_entity.dict()
                )

                assert put_sample_request.status_code == 200

        for folder_name, folder in folders_map.items():
            reader = UnderfolderReader(folder)
            for sample in reader:
                # checks for updateded metadata
                assert sample["metadata"]["put"] == "inception"
                assert len(sample["metadatay"]) == 5

    def test_api_auth(self, sample_underfolder_minimnist, tmp_path):

        # secrets
        shared_secret = "secr3t!"
        invalid_secret = "invalid_secret!"

        # Fake hardcoded auth callback
        def auth_callback(token: str) -> bool:
            nonlocal shared_secret
            return token == shared_secret

        # creates underfolders from minimnist sample data
        folders_names = ["protected_dataset"]
        folders_map = {}
        for folder_name in folders_names:
            folder_path = tmp_path / folder_name
            shutil.copytree(sample_underfolder_minimnist["folder"], folder_path)
            folders_map[folder_name] = str(folder_path)

        # creates underfolder api
        api = UnderfolderAPI(underfolders_map=folders_map, auth_callback=auth_callback)

        # creates the API client
        client = TestClient(api)
        assert client is not None

        baked_requests = [
            {"method": "GET", "url": "/datasets"},
            {"method": "GET", "url": "/dataset/protected_dataset"},
            {"method": "GET", "url": "/dataset/protected_dataset/0"},
            {"method": "GET", "url": "/dataset/protected_dataset/0/image"},
            {
                "method": "PUT",
                "url": "/dataset/protected_dataset/0",
                "json": EntitySample(
                    id=0,
                    metadata={"metadata": {"put": "inception"}},
                    data={},
                ).dict(),
            },
        ]

        for backed_request in baked_requests:

            # No Auth mechanism set, should fail with 401 UNATHORIZED
            with pytest.raises(HTTPException) as ex_info:
                response = client.request(**backed_request)
            assert isinstance(ex_info.value, HTTPException)
            assert ex_info.value.status_code == 401
            rich.print(backed_request, ex_info.value.status_code)

            #  Auth mechanism set but invalid TOKEN
            with pytest.raises(HTTPException) as ex_info:
                response = client.request(
                    **backed_request,
                    headers={"Authorization": "Bearer " + invalid_secret},
                )
            assert isinstance(ex_info.value, HTTPException)
            rich.print(backed_request, ex_info.value.status_code)

            # Auth mechanism set and valid token
            response = client.request(
                **backed_request,
                headers={"Authorization": "Bearer " + shared_secret},
            )
            assert response.status_code == 200
            rich.print(backed_request, response.status_code)


class TestUnderfolderAPISearch:
    @pytest.mark.parametrize(
        "search_item",
        [
            {
                "metadata": {
                    "info": {
                        "sample_id": f"{DictSearch.KEY_PLACEHOLDER} >= 1 AND {DictSearch.KEY_PLACEHOLDER} <= 9"
                    },
                },
                "expected": 3,
                "pagination": {"offset": 5, "limit": 3},
            },
            {
                "metadata": {
                    "info": {
                        "sample_id": f"{DictSearch.KEY_PLACEHOLDER} >= 1 AND {DictSearch.KEY_PLACEHOLDER} <= 9"
                    },
                },
                "expected": 0,
                "pagination": {"offset": 20, "limit": 30},
            },
            {
                "metadata": {
                    "info": {
                        "sample_id": f"{DictSearch.KEY_PLACEHOLDER} >= 1 AND {DictSearch.KEY_PLACEHOLDER} <= 9"
                    },
                },
                "expected": 3,
                "pagination": {"offset": 0, "limit": 3},
            },
        ],
    )
    def test_api_search_samples_basic_pagination(
        self, sample_underfolder_minimnist_queries, tmp_path, search_item
    ):

        # creates underfolders from minimnist sample data
        dataset_name = "searchable"
        folder = str(tmp_path / dataset_name)
        shutil.copytree(sample_underfolder_minimnist_queries["folder"], folder)

        # creates the api
        api = UnderfolderAPI(
            underfolders_map={dataset_name: folder}, auth_callback=None
        )

        # creates the API client
        client = TestClient(api)

        # Search ALL
        metadata = search_item["metadata"]
        expected_count = search_item["expected"]
        pagination = search_item["pagination"]

        search_entity = EntitySampleSearchRequest(
            proto_sample=EntitySample(id=-1, metadata=metadata, data={}),
            pagination=EntityPagination(**pagination),
        )

        search_response = client.post(
            f"/search/{dataset_name}", json=search_entity.dict()
        )
        assert search_response.status_code == 200
        response = EntitySampleSearchResponse(**search_response.json())
        assert len(response.samples) == expected_count
        for sample_entity in response.samples:
            assert isinstance(sample_entity, EntitySample)

        direct_samples_entities = UnderfolderInterface(
            "interface", folder
        ).search_samples(search_entity.proto_sample)

        expected_ids = [x.id for x in direct_samples_entities]
        for response_entity_sample in response.samples:
            assert response_entity_sample.id in expected_ids

    def test_api_search_samples_pagination_consistency(
        self, sample_underfolder_minimnist_queries, tmp_path
    ):

        # creates underfolders from minimnist sample data
        dataset_name = "searchable"
        folder = str(tmp_path / dataset_name)
        shutil.copytree(sample_underfolder_minimnist_queries["folder"], folder)

        # creates the api
        api = UnderfolderAPI(
            underfolders_map={dataset_name: folder}, auth_callback=None
        )

        reader = UnderfolderReader(folder)

        whole_samples_ids = set([x.id for x in reader])

        # creates the API client
        client = TestClient(api)

        # Search ALL
        total = len(reader)

        # NO data request for pagination discovery
        search_entity = EntitySampleSearchRequest(
            proto_sample=EntitySample(id=-1, metadata={}, data={}),
            pagination=None,
            only_pagination=True,
        )

        search_response = client.post(
            f"/search/{dataset_name}", json=search_entity.dict()
        )
        assert search_response.status_code == 200
        response = EntitySampleSearchResponse(**search_response.json())

        # total count
        total_count = response.pagination.total_count
        assert total_count == total

        # split request in several paginations modes, and check consistency
        page_sizes = list(range(1, total_count))
        for page_size in page_sizes:

            collected_samples = []
            for offset in range(0, total_count, page_size):
                search_entity = EntitySampleSearchRequest(
                    proto_sample=EntitySample(id=-1, metadata={}, data={}),
                    pagination=EntityPagination(offset=offset, limit=page_size),
                )

                search_response = client.post(
                    f"/search/{dataset_name}", json=search_entity.dict()
                )
                assert search_response.status_code == 200
                response = EntitySampleSearchResponse(**search_response.json())
                collected_samples.extend(response.samples)

            assert len(collected_samples) == total_count

            # checks all ids are present
            collected_ids = set([x.id for x in collected_samples])
            assert collected_ids == whole_samples_ids
