from pipelime.sequences.api.authentication import CustomAPIAuthentication
from pipelime.sequences.operations import OperationFilterByScript
from pipelime.sequences.api.entities import (
    EntityDataset,
    EntityPagination,
    EntitySample,
    EntitySampleData,
    EntitySampleSearchRequest,
    EntitySampleSearchResponse,
)
from pipelime.sequences.api.base import (
    SequenceInterface,
)
from pipelime.sequences.samples import FileSystemSample, Sample
from pipelime.sequences.streams.underfolder import UnderfolderStream
from pipelime.filesystem.toolkit import FSToolkit
from pipelime.tools.bytes import DataCoding

from typing import Any, Dict, Callable, Optional, Sequence
from fastapi import HTTPException
from fastapi.param_functions import Depends
from fastapi.routing import APIRouter
from starlette.responses import StreamingResponse
from starlette.requests import Request
import io
from loguru import logger
from pipelime.tools.dictionaries import DictSearch


class UnderfolderInterface(SequenceInterface):
    def __init__(
        self, name: str, folder: str, allowed_keys: Optional[Sequence[str]] = None
    ) -> None:
        """The UnderfolderInterface is a SequenceInterface that uses Entities to
        'communicate' with the underlying UnderfolderStream (i.e. an UnderfolderReader
        and UnderfolderWriter wrapper).


        :param name: name of the interface (should be unique among all interfaces)
        :type name: str
        :param folder: the Underfolder path
        :type folder: str
        :param allowed_keys: the list of allowed keys, if NONE all keys are allowed,
        default None
        :type allowed_keys: Optional[Sequence[str]], optional
        """
        self._name = name
        self._stream = UnderfolderStream(folder=folder, allowed_keys=allowed_keys)

    def _raw_sample_to_entity(self, raw_sample: Sample) -> EntitySample:
        """Converts a raw sample into an entity sample.

        :param raw_sample: the raw sample
        :type raw_sample: Sample
        :return: the entity sample
        :rtype: EntitySample
        """

        # create entity
        entity = EntitySample(id=raw_sample.id, metadata={}, data={})

        # fill metadata and urls
        for key in raw_sample:
            filename = raw_sample.filesmap[key]
            extension = FSToolkit.get_file_extension(filename)
            if DataCoding.is_metadata_extension(extension):
                entity.metadata[key] = raw_sample[key]
            else:
                # TODO: move this kind of "File Description" in unique proxy
                if DataCoding.is_image_extension(extension):
                    entity.data[key] = EntitySampleData(
                        type="image", encoding=extension
                    )
                elif DataCoding.is_numpy_extension(extension):
                    entity.data[key] = EntitySampleData(
                        type="numpy", encoding=extension
                    )
                elif DataCoding.is_pickle_extension(extension):
                    entity.data[key] = EntitySampleData(
                        type="pickle", encoding=extension
                    )
                elif DataCoding.is_text_extension(extension):
                    entity.data[key] = EntitySampleData(type="text", encoding=extension)

        return entity

    def _get_sample_entity(self, sample_id: int) -> EntitySample:
        """Returns the sample entity for the given sample id. Wraps all raw metadata
        into the EntitySample. For binary data just the item names and encoding will
        be provided through the entity 'data' field

        :param sample_id: the sample id
        :type sample_id: int
        :return: the sample entity built
        :rtype: EntitySample
        """

        # get raw sample
        raw_sample: FileSystemSample = self._stream.get_sample(sample_id)

        return self._raw_sample_to_entity(raw_sample)

    def _put_sample_entity(self, sample_id: int, entity: EntitySample) -> None:
        """Updates the raw sample with the given entity.

        :param sample_id: the sample id
        :type sample_id: int
        :param entity: the sample entity to update
        :type entity: EntitySample
        :raises PermissionError: if some metadata is not allowed to be updated
        :raises KeyError: if sample_id is not found
        """

        try:
            for key in entity.metadata:
                self._stream.set_data(
                    sample_id, key, entity.metadata[key], format="dict"
                )

        except PermissionError as e:
            raise PermissionError(e)

        except KeyError as e:
            raise KeyError(e)

    def get_dataset(self) -> EntityDataset:
        """Returns the dataset entity. Wraps dataset info into the EntityDataset.

        :return: the dataset entity
        :rtype: EntityDataset
        """

        return EntityDataset(name=self._name, manifest=self._stream.manifest())

    def get_sample(self, sample_id: int) -> EntitySample:
        """Returns the sample entity for the given sample id. Wraps all raw metadata

        :param sample_id: the sample id
        :type sample_id: int
        :raises KeyError: if sample_id is not found
        :return: the sample entity built
        :rtype: EntitySample
        """

        if sample_id in self._stream.get_sample_ids():
            return self._get_sample_entity(sample_id)
        else:
            raise KeyError(f"Sample {sample_id} not found")

    def get_sample_data(
        self, sample_id: int, item_name: str, format: str = None
    ) -> io.BytesIO:
        """Returns the raw binary data for the given sample id and item name.

        :param sample_id: the sample id
        :type sample_id: int
        :param item_name: the item name
        :type item_name: str
        :param format: binary data forma (e.g. jpg, png) , defaults to None
        :type format: str, optional
        :raises KeyError: if sample_id is not found
        :raises KeyError: if item_name is not found
        :return: io.BytesIO buffer
        :rtype: io.BytesIO
        """

        if sample_id in self._stream.get_sample_ids():
            entity = self._get_sample_entity(sample_id)

            if item_name in entity.data:
                if format is None:
                    return self._stream.get_bytes(sample_id, item_name)
                else:
                    raise NotImplementedError(
                        "Custom format not implemented yet, leave NONE for auto encoding"
                        "based on filename extension!"
                    )
                    # item = entity.data[item_name]
                    # data_format = format if format is not None else item["encoding"]
                    # return self._stream.get_data(sample_id, item_name, format=data_format)
            else:
                raise KeyError(f"Item {item_name} not found")
        else:
            raise KeyError(f"Sample {sample_id} not found")

    def put_sample(self, sample_id: int, sample_entity: EntitySample) -> EntitySample:
        """Updates the raw sample with the given entity.

        :param sample_id: the sample id
        :type sample_id: int
        :param sample_entity: the sample entity to update
        :type sample_entity: EntitySample
        :raises PermissionError: if some metadata is not allowed to be updated
        :raises KeyError: if sample_id is not found
        :raises KeyError: if item_name is not found
        :return: the sample entity built
        :rtype: EntitySample
        """

        if sample_id in self._stream.get_sample_ids():

            try:

                self._put_sample_entity(sample_id, sample_entity)
                return self._get_sample_entity(sample_id)

            except PermissionError as e:
                raise PermissionError(e)

        else:

            raise KeyError(f"Sample {sample_id} not found")

    def search_samples(self, proto_sample: EntitySample) -> Sequence[EntitySample]:
        """Search samples based on the given sample entity.

        :param proto_sample: the sample entity with query values
        :type proto_sample: EntitySample
        :return: the list of samples matching the query
        :rtype: Sequence[EntitySample]
        """

        def filter_sample(sample: FileSystemSample, sequence) -> bool:
            return DictSearch.match_queries(proto_sample.metadata, sample)

        filtered_reader = OperationFilterByScript(path_or_func=filter_sample)(
            self._stream.reader
        )

        entities = [self._raw_sample_to_entity(sample) for sample in filtered_reader]
        return sorted(entities, key=lambda x: x.id)


class UnderfolderAPI(APIRouter):
    def __init__(
        self,
        underfolders_map: Dict[str, str],
        allowed_keys_map: Optional[Dict[str, Sequence[str]]] = None,
        auth_callback: Callable[[str], bool] = None,
    ):
        """Initialize the API router with a map of underfolder names to underfolder paths.

        :param underfolders_map: A map of underfolder names to underfolder paths.
        :type underfolders_map: Dict[str, str]
        :param allowed_keys_map: A map of underfolder names to allowed keys.
        :type allowed_keys_map: Dict[str, Sequence[str]]
        :param auth_callback: A callback function that takes a token and returns a boolean
            indicating whether the token is valid.
        :type auth_callback: Callable[[str], bool]
        """

        super().__init__()

        self._underfolders_map = underfolders_map

        # Authentication callback verification
        self._auth_callback: Optional[Callable[[str], bool]] = auth_callback

        # Single underfodlers interfaces
        self._interfaces_map: Dict[str, UnderfolderInterface] = {}
        for name, folder in self._underfolders_map.items():
            allowed_keys = (
                allowed_keys_map[name]
                if (allowed_keys_map is not None and name in allowed_keys_map)
                else None
            )
            interface = UnderfolderInterface(name, folder, allowed_keys=allowed_keys)
            self._interfaces_map[name] = interface

            logger.info(
                f"New API Interface created: (name={name}, folder={str(folder)})"
            )

        # .------------------.
        # | API list_dataset |
        # '------------------'
        # generated with: https://texteditor.com/ascii-frames/

        self.add_api_route(
            "/datasets",
            response_model=Dict[str, EntityDataset],
            endpoint=self.list_datasets,
            methods=["GET"],
            dependencies=self._get_dependencies(),
        )

        # .-----------------.
        # | API get_dataset |
        # '-----------------'
        self.add_api_route(
            "/dataset/{dataset_name}",
            response_model=EntityDataset,
            endpoint=self.get_dataset,
            methods=["GET"],
            dependencies=self._get_dependencies(),
        )

        # .----------------.
        # | API get_sample |
        # '----------------'
        self.add_api_route(
            "/dataset/{dataset_name}/{sample_id}",
            response_model=EntitySample,
            endpoint=self.get_sample,
            methods=["GET"],
            dependencies=self._get_dependencies(),
        )

        # .---------------------.
        # | API get_sample_data |
        # '---------------------'
        self.add_api_route(
            "/dataset/{dataset_name}/{sample_id}/{item_name}",
            response_model=Any,
            endpoint=self.get_sample_data,
            methods=["GET"],
            dependencies=self._get_dependencies(),
        )

        # .----------------.
        # | API put_sample |
        # '----------------'
        self.add_api_route(
            "/dataset/{dataset_name}/{sample_id}",
            response_model=EntitySample,
            endpoint=self.put_sample,
            methods=["PUT"],
            dependencies=self._get_dependencies(),
        )

        # .-------------------.
        # | API search_sample |
        # '-------------------'
        self.add_api_route(
            "/search/{dataset_name}",
            response_model=EntitySampleSearchResponse,
            endpoint=self.search_samples,
            methods=["POST"],
            dependencies=self._get_dependencies(),
        )

    def _log_api(
        self,
        request: Request,
        message: str = "",
    ):
        import inspect

        message_part = f" [{message}] " if message else ""
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        logger.info(
            f"[{request.method}] {calframe[1][3]}{message_part}<- {request.client}"
        )
        logger.debug(
            f"[{request.method}] {calframe[1][3]} path_params: {request.path_params}"
        )
        logger.debug(
            f"[{request.method}] {calframe[1][3]} query_params: {request.query_params}"
        )

    def _get_dependencies(self):
        dependencies: Sequence[Depends] = []

        # Adds authentication if callback is provided
        if self._auth_callback is not None:
            dependencies.append(
                Depends(CustomAPIAuthentication(token_callback=self._auth_callback))
            )

        return dependencies

    async def list_datasets(self, request: Request) -> Dict[str, EntityDataset]:
        """List all datasets as a dictionary of dataset names to dataset entity.

        :param request: The request object.
        :type request: Request
        :return: A dictionary of dataset names to dataset entity.
        :rtype: Dict[str, EntityDataset]
        """

        self._log_api(request=request)

        return {
            name: interface.get_dataset()
            for name, interface in self._interfaces_map.items()
        }

    async def get_dataset(self, dataset_name: str, request: Request) -> EntityDataset:
        """Get a dataset entity by name.

        :param dataset_name: The name of the dataset.
        :type dataset_name: str
        :param request: The request object.
        :type request: Request
        :raises HTTPException: If the dataset is not found.
        :return: The retreived dataset entity.
        :rtype: EntityDataset
        """

        self._log_api(request=request)

        if dataset_name in self._interfaces_map:

            return self._interfaces_map[dataset_name].get_dataset()

        else:

            raise HTTPException(
                status_code=404, detail=f"Dataset {dataset_name} not found"
            )

    async def get_sample(
        self, dataset_name: str, sample_id: int, request: Request
    ) -> EntitySample:
        """Get a sample entity by id and dataset name.

        :param dataset_name: The name of the dataset.
        :type dataset_name: str
        :param sample_id: The id of the sample.
        :type sample_id: int
        :param request: The request object.
        :type request: Request
        :raises HTTPException: If the sample is not found.
        :raises HTTPException: If the dataset is not found.
        :return: The retreived sample entity.
        :rtype: EntitySample
        """

        self._log_api(request=request)

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
        self,
        dataset_name: str,
        sample_id: int,
        item_name: str,
        request: Request,
        format: str = None,
    ) -> StreamingResponse:
        """Get a sample binary data by id and dataset name as a StreamingResponse.

        :param dataset_name: The name of the dataset.
        :type dataset_name: str
        :param sample_id: The id of the sample.
        :type sample_id: int
        :param item_name:  The name of the item.
        :type item_name: str
        :param request: The request object.
        :type request: Request
        :param format: binary data encoding (e.g. jpg, png, pkl) , defaults to None
        :type format: str, optional
        :raises HTTPException: If the sample is not found.
        :raises HTTPException: If the dataset is not found.
        :raises HTTPException: If the item is not found.
        :return: binary data stream with selected mimetype
        :rtype: StreamingResponse
        """

        self._log_api(request=request)

        if dataset_name in self._interfaces_map:
            try:

                data, mimetype = self._interfaces_map[dataset_name].get_sample_data(
                    sample_id=sample_id, item_name=item_name, format=format
                )

                return StreamingResponse(data, media_type=mimetype)

            except KeyError as e:

                raise HTTPException(status_code=404, detail=str(e))

        else:

            raise HTTPException(
                status_code=404, detail=f"Dataset {dataset_name} not found"
            )

    async def put_sample(
        self,
        dataset_name: str,
        sample_id: int,
        sample_entity: EntitySample,
        request: Request,
    ) -> EntitySample:
        """Updates a sample entity by id and dataset name. The body of the request should
        be a JSON serialized EntitySample object

        :param dataset_name: The name of the dataset.
        :type dataset_name: str
        :param sample_id: The id of the sample.
        :type sample_id: int
        :param sample_entity: The sample entity data to update.
        :type sample_entity: EntitySample
        :param request: The request object.
        :type request: Request
        :raises HTTPException: If the sample is not found.
        :raises HTTPException: If the dataset is not found.
        :return: The retrieved sample entity after update operation.
        :rtype: EntitySample
        """

        self._log_api(request=request)

        if dataset_name in self._interfaces_map:
            try:
                return self._interfaces_map[dataset_name].put_sample(
                    sample_id, sample_entity
                )
            except KeyError as e:
                raise HTTPException(status_code=404, detail=str(e))
        else:
            raise HTTPException(status_code=404, detail=f"{dataset_name} not found")

    async def search_samples(
        self,
        dataset_name: str,
        request: Request,
        search_entity: EntitySampleSearchRequest,
    ) -> EntitySampleSearchResponse:

        self._log_api(request=request)

        if dataset_name in self._interfaces_map:

            try:
                samples_entities = self._interfaces_map[dataset_name].search_samples(
                    search_entity.proto_sample
                )

                if search_entity.only_pagination:
                    pagination = EntityPagination.create_from_sequence(samples_entities)
                    return EntitySampleSearchResponse(pagination=pagination)
                else:
                    filtered = search_entity.pagination.filter(samples_entities)
                    return EntitySampleSearchResponse(
                        samples=filtered, pagination=search_entity.pagination
                    )
            except Exception:
                import traceback

                raise HTTPException(status_code=500, detail=f"{traceback.format_exc()}")
        else:

            raise HTTPException(
                status_code=404, detail=f"Dataset {dataset_name} not found"
            )
