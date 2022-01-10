import io
import shutil
from _pytest._code.code import ExceptionInfo
from fastapi.exceptions import HTTPException
from fastapi.security.oauth2 import OAuth2PasswordBearer

import imageio
from pipelime.sequences.api.base import EntityDataset, EntitySample, ParamPagination
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.api.underfolder import UnderfolderAPI
from fastapi.testclient import TestClient
import pytest
import rich
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN

from pipelime.tools.dictionaries import DictSearch


class TestUnderfolderAPI:
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
                get_wrong_dataset_response = client.get(f"/dataset/IMPOSSIBLE_NAME!@/")

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
            ex_info: ExceptionInfo
            assert isinstance(ex_info.value, HTTPException)
            assert ex_info.value.status_code == 401
            rich.print(backed_request, ex_info.value.status_code)

            #  Auth mechanism set but invalid TOKEN
            with pytest.raises(HTTPException) as ex_info:
                response = client.request(
                    **backed_request,
                    headers={"Authorization": "Bearer " + invalid_secret},
                )
            ex_info: ExceptionInfo
            assert isinstance(ex_info.value, HTTPException)
            rich.print(backed_request, ex_info.value.status_code)

            # Auth mechanism set and valid token
            response = client.request(
                **backed_request,
                headers={"Authorization": "Bearer " + shared_secret},
            )
            assert response.status_code == 200
            rich.print(backed_request, response.status_code)

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
                "pagination": {"paginationStart": 5, "paginationSize": 3},
            },
            {
                "metadata": {
                    "info": {
                        "sample_id": f"{DictSearch.KEY_PLACEHOLDER} >= 1 AND {DictSearch.KEY_PLACEHOLDER} <= 9"
                    },
                },
                "expected": 0,
                "pagination": {"paginationStart": 20, "paginationSize": 30},
            },
            {
                "metadata": {
                    "info": {
                        "sample_id": f"{DictSearch.KEY_PLACEHOLDER} >= 1 AND {DictSearch.KEY_PLACEHOLDER} <= 9"
                    },
                },
                "expected": 3,
                "pagination": {"paginationStart": 0, "paginationEnd": 2},
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

        search_response = client.get(
            f"/search/{dataset_name}",
            json=EntitySample(id=-1, metadata=metadata, data={}).dict(),
            params=ParamPagination(**pagination).dict(),
        )
        assert search_response.status_code == 200
        samples_entities = [EntitySample(**x) for x in search_response.json()]
        assert len(samples_entities) == expected_count

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

        # creates the API client
        client = TestClient(api)

        # Search ALL
        total = len(reader)

        # Split all results with different paginations params
        paginations = [
            [0, -1, 5],  # Start 0, Size -1, End 5,
            [6, -1, 8],  # Start 6, Size -1, End 8,
            [9, -1, 10],  # Start 9, Size -1, End 10,
            [11, 9, None],  # Start 11, Size 9, End None,
        ]

        # build params
        paginations_params = [
            ParamPagination(paginationStart=a, paginationSize=b, paginationEnd=c)
            for a, b, c in paginations
        ]

        whole_samples = []

        # SEARCH ALL Samples (wihtout query) but with pagination
        for pagination_params in paginations_params:
            rich.print("PAGINATION", pagination_params.dict())

            search_response = client.get(
                f"/search/{dataset_name}",
                json=EntitySample(id=-1, metadata={}, data={}).dict(),
                params=pagination_params.dict(),
            )
            assert search_response.status_code == 200
            samples_entities = [EntitySample(**x) for x in search_response.json()]
            whole_samples += samples_entities
            rich.print("IDS", [x.id for x in samples_entities])

        assert len(whole_samples) == total