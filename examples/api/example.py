from re import U
from typing import Any, Dict, Hashable, List
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import traceback

from fastapi.routing import APIRouter
from starlette.responses import StreamingResponse
from pipelime.sequences.api.base import EntityDataset, EntitySample
from pipelime.sequences.api.underfolder import (
    UnderfolderInterface,
)
from fastapi_utils.cbv import cbv

from pipelime.sequences.streams.underfolder import UnderfolderStream


class UnderfolderAPI(APIRouter):
    DATASET_PREFIX = "dataset"

    def __init__(self, underfolders_map: Dict[str, str]):
        super().__init__()

        self._underfolders_map = underfolders_map

        # Single underfodlers interfaces
        self._interfaces_map = {
            name: UnderfolderInterface(name, folder)
            for name, folder in self._underfolders_map.items()
        }

        self.add_api_route(
            "/datasets",
            response_model=Dict[str, EntityDataset],
            endpoint=self.list_datasets,
            methods=["GET"],
        )

        self.add_api_route(
            "/dataset/{dataset_name}",
            response_model=EntityDataset,
            endpoint=self.get_dataset,
            methods=["GET"],
        )

        self.add_api_route(
            "/dataset/{dataset_name}/{sample_id}",
            response_model=EntitySample,
            endpoint=self.get_sample,
            methods=["GET"],
        )

        self.add_api_route(
            "/dataset/{dataset_name}/{sample_id}/{item_name}",
            response_model=Any,
            endpoint=self.get_sample_data,
            methods=["GET"],
        )

        self.add_api_route(
            "/dataset/{dataset_name}/{sample_id}",
            response_model=EntitySample,
            endpoint=self.put_sample,
            methods=["PUT"],
        )

    async def list_datasets(self):

        return {
            name: interface.get_dataset()
            for name, interface in self._interfaces_map.items()
        }

    async def get_dataset(self, dataset_name: str):
        if dataset_name in self._interfaces_map:
            return self._interfaces_map[dataset_name].get_dataset()
        else:
            raise HTTPException(
                status_code=404, detail=f"Dataset {dataset_name} not found"
            )

    async def get_sample(self, dataset_name: str, sample_id: int):
        if dataset_name in self._interfaces_map:
            try:
                return self._interfaces_map[dataset_name].get_sample(sample_id)
            except KeyError as e:
                raise HTTPException(status_code=404, detail=str(e))
        else:
            raise HTTPException(
                status_code=404, detail=f"Dataset {dataset_name} not found"
            )

    async def get_sample_data(
        self, dataset_name: str, sample_id: int, item_name: str, format: str = None
    ):
        if dataset_name in self._interfaces_map:
            try:
                item_data, item_mimetype = self._interfaces_map[
                    dataset_name
                ].get_sample_data(
                    sample_id=sample_id, item_name=item_name, format=format
                )

                return StreamingResponse(item_data, media_type=item_mimetype)
            except KeyError as e:
                raise HTTPException(status_code=404, detail=str(e))
            except ValueError as e:
                raise HTTPException(status_code=405, detail=str(e))
        else:
            raise HTTPException(
                status_code=404, detail=f"Dataset {dataset_name} not found"
            )

    async def put_sample(
        self, dataset_name: str, sample_id: int, sample_entity: EntitySample
    ):
        if dataset_name in self._interfaces_map:
            try:
                return self._interfaces_map[dataset_name].put_sample(
                    sample_id, sample_entity
                )
            except KeyError as e:
                raise HTTPException(status_code=404, detail=str(e))
        else:
            raise HTTPException(status_code=404, detail=f"{dataset_name} not found")


mapp = UnderfolderAPI(
    {
        "fake": "/Users/danieledegregorio/Desktop/experiments/2021-12-09.PirelliFirstDataset/datasets/underfolder/crops",
        "fake2": "/tmp/fakedataset",
        "fake3": "/tmp/fakedataset",
    }
)

# uapp = UnderfolderAPI(name="fake", folder="/tmp/fakedataset")

app = FastAPI()
app.include_router(mapp)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# uvicorn.run(
#     "app",
#     host="0.0.0.0",
#     port=8000,
#     reload=True,
#     debug=True,
#     # workers=3,
# )
