from abc import ABCMeta, abstractmethod
from pathlib import Path, WindowsPath
from typing import Type, Mapping, MutableMapping, Any, Tuple, Union, Optional, BinaryIO
import hashlib
from urllib.parse import urlparse, unquote_plus, ParseResult

import os
import shutil

from loguru import logger


class RemoteRegister(ABCMeta):
    """A factory class managing a single remote instance
    for any (scheme, netloc) pair.
    """

    REMOTE_CLASSES: MutableMapping[str, Type["BaseRemote"]] = {}
    REMOTE_INSTANCES: MutableMapping[Tuple[str, str], "BaseRemote"] = {}

    def __init__(cls, name, bases, dct):
        cls.REMOTE_CLASSES[cls.scheme()] = cls  # type: ignore
        super().__init__(name, bases, dct)

    @classmethod
    def get_instance(cls, scheme: str, netloc: str, **kwargs) -> Optional["BaseRemote"]:
        """Return a remote instance for this scheme and netloc.

        :param scheme: the protocol name, eg, 's3' or 'file'.
        :type scheme: str
        :param netloc: the address of the endpoint.
        :type netloc: str
        :param **kwargs: arguments forwarded to the remote instance `__init__()`
        :return: the remote instance or None if the scheme is unknown.
        :rtype: Optional[BaseRemote]
        """
        remote_instance = cls.REMOTE_INSTANCES.get((scheme, netloc))
        if remote_instance is None:
            remote_class = cls.REMOTE_CLASSES.get(scheme)
            if remote_class is not None:
                remote_instance = remote_class(netloc, **kwargs)
                cls.REMOTE_INSTANCES[(scheme, netloc)] = remote_instance
            else:
                logger.warning(f"Unknown remote scheme '{scheme}'.")
        return remote_instance


def create_remote(scheme: str, netloc: str, **kwargs) -> Optional["BaseRemote"]:
    """Return a remote instance for this scheme and netloc.

    :param scheme: the protocol name, eg, 's3' or 'file'.
    :type scheme: str
    :param netloc: the address of the endpoint.
    :type netloc: str
    :param **kwargs: arguments forwarded to the remote instance's `__init__()`
    :return: the remote instance or None if the scheme is unknown.
    :rtype: Optional[BaseRemote]
    """
    return RemoteRegister.get_instance(scheme, netloc, **kwargs)


def parse_url(url: str) -> Union[Tuple[str, str], Tuple[None, None]]:
    """Extract base path and target name from the path component of a given url.

    :param url: the url to split.
    :type url: str
    :return: the base path and the target name.
    :rtype: Union[Tuple[str, str], Tuple[None, None]]
    """
    parsed_url = urlparse(url)
    if len(parsed_url.path) > 1:
        file_full_path = Path(unquote_plus(parsed_url.path)[1:])
        return str(file_full_path.parent.as_posix()), str(file_full_path.name)
    else:
        return None, None


def get_remote_and_paths(
    url: str, remotes_kwargs: Mapping[str, Mapping[str, Any]] = {}
) -> Tuple[Optional["BaseRemote"], Optional[str], Optional[str]]:
    """Create a remote and extract base and target path from a given url.

    :param url: the url to split.
    :type url: str
    :param remotes_kwargs: keys are the scheme names and values are the arguments
        that must be forwarded to the remote instance's `__init__()`.
    :type remotes_kwargs: Mapping[str, Mapping[str, Any]]
    :return: (remote, base path, target name)
    :rtype: Tuple[Optional["BaseRemote"], Optional[str], Optional[str]]
    """
    parsed_url = urlparse(url)
    return (
        create_remote(
            parsed_url.scheme,
            parsed_url.netloc,
            **(remotes_kwargs.get(parsed_url.scheme, {})),
        ),
    ) + parse_url(url)


class BaseRemote(metaclass=RemoteRegister):  # type: ignore
    """Base class for any remote."""

    def __init__(self, netloc: str):
        """Set the network address.

        :param netloc: the network address.
        :type netloc: str
        """
        self._netloc = netloc

    def upload_file(
        self, local_file: Union[Path, str], target_base_path: str
    ) -> Optional[str]:
        """Upload an existing file to remote storage.

        :param local_file: local file path.
        :type local_file: Union[Path, str]
        :param target_base_path: the remote base path, eg, the bucket name.
        :type target_base_path: str
        :return: a url to get back the file.
        :rtype: Optional[str]
        """
        local_file = Path(local_file)
        file_size = local_file.stat().st_size
        with open(local_file, "rb") as file_data:
            return self.upload_stream(
                file_data, file_size, target_base_path, "".join(local_file.suffixes)
            )

    def upload_stream(
        self,
        local_stream: BinaryIO,
        local_stream_size: int,
        target_base_path: str,
        target_suffix: str,
    ) -> Optional[str]:
        """Upload data from a readable stream.

        :param local_stream: a binary data stream.
        :type local_stream: BinaryIO
        :param local_stream_size: the byte size to upload.
        :type local_stream_size: int
        :param target_base_path: the remote base path, eg, the bucket name.
        :type target_base_path: str
        :param target_suffix: the remote file suffix.
        :type target_suffix: str
        :return: a url to get back the file.
        :rtype: Optional[str]
        """
        hash_name = self._compute_hash(
            local_stream, self._get_hash_fn(target_base_path)
        )
        target_name = hash_name + target_suffix
        if self.target_exists(target_base_path, target_name) or self._upload(
            local_stream, local_stream_size, target_base_path, target_name
        ):
            return self._make_url(f"{target_base_path}/{target_name}")
        return None

    def download_file(
        self,
        local_file: Union[Path, str],
        source_base_path: str,
        source_name: str,
    ) -> bool:
        """Download and write data to a local file.

        :param local_file: the file to write to.
        :type local_file: Union[Path, str]
        :param source_base_path: the remote base path, eg, the bucket name.
        :type source_base_path: str
        :param source_name: the remote object name.
        :type source_name: str
        :return: True if no error occurred.
        :rtype: bool
        """
        local_file = Path(local_file)
        source_name_path = Path(source_name)
        if local_file.suffixes != source_name_path.suffixes:
            local_file = local_file.with_suffix("".join(source_name_path.suffixes))
        if local_file.is_file():
            with open(local_file, "rb") as local_stream:
                hash_name = self._compute_hash(
                    local_stream, self._get_hash_fn(source_base_path)
                )
                if (
                    hash_name
                    == source_name_path.name[: -len("".join(source_name_path.suffixes))]
                ):
                    # local file is up-to-date
                    return True

        local_file.parent.mkdir(parents=True, exist_ok=True)
        part_file = local_file.with_suffix(local_file.suffix + ".part")
        offset: int = 0
        try:
            offset = part_file.stat().st_size
        except IOError:
            pass

        ok = False
        with open(part_file, "ab") as part_stream:
            ok = self.download_stream(
                part_stream, source_base_path, source_name, offset
            )

        if ok:
            local_file.unlink(missing_ok=True)
            part_file.rename(local_file)

        return ok

    def download_stream(
        self,
        local_stream: BinaryIO,
        source_base_path: str,
        source_name: str,
        source_offset: int = 0,
    ) -> bool:
        """Download and write data to a writable stream.

        :param local_stream: a writable stream.
        :type local_stream: BinaryIO
        :param source_base_path: the remote base path, eg, the bucket name.
        :type source_base_path: str
        :param source_name: the remote object name.
        :type source_name: str
        :param source_offset: where to start reading the remote data, defaults to 0
        :type source_offset: int, optional
        :return: True if no error occurred.
        :rtype: bool
        """
        return self._download(
            local_stream, source_base_path, source_name, source_offset
        )

    @abstractmethod
    def target_exists(self, target_base_path: str, target_name: str) -> bool:
        """Check if a remote object exists.

        :param target_base_path: the remote base path, eg, the bucket name.
        :type target_base_path: str
        :param target_name: the remote object name.
        :type target_name: str
        :return: True if <target_base_path>/<target_name> exists on this remote.
        :rtype: bool
        """
        ...

    def _compute_hash(self, stream: BinaryIO, hash_fn: Any = None) -> str:
        """Compute a hash value based on the content of a readable stream.

        :param stream: the stream to hash.
        :type stream: BinaryIO
        :param hash_fn: the hash funtion, if None `hashlib.sha256()` will be used,
            defaults to None
        :type hash_fn: Any, optional
        :return: the computed hash
        :rtype: str
        """
        if hash_fn is None:
            hash_fn = hashlib.sha256()
        b = bytearray(1024 * 1024)
        mv = memoryview(b)
        fpos = stream.tell()
        for n in iter(lambda: stream.readinto(mv), 0):  # type: ignore
            hash_fn.update(mv[:n])
        stream.seek(fpos)
        return hash_fn.hexdigest()

    def _make_url(self, target_full_path: str) -> str:
        """Make a complete url for this remote path.

        :param target_full_path: the remote path.
        :type target_full_path: str
        :return: the url
        :rtype: str
        """
        return ParseResult(
            scheme=self.scheme(),
            netloc=self.netloc,
            path=Path(target_full_path).as_posix(),
            params="",
            query="",
            fragment="",
        ).geturl()

    @abstractmethod
    def _get_hash_fn(self, target_base_path: str) -> Any:
        """Return the hash function used for the object in the give remote base path.

        :param target_base_path: the remote base path, eg, the bucket name.
        :type target_base_path: str
        :return: the hash function, eg, `hashlib.sha256()`.
        :rtype: Any
        """
        ...

    @abstractmethod
    def _upload(
        self,
        local_stream: BinaryIO,
        local_stream_size: int,
        target_base_path: str,
        target_name: str,
    ) -> bool:
        """Upload data from a readable stream.

        :param local_stream: a binary data stream.
        :type local_stream: BinaryIO
        :param local_stream_size: the byte size to upload.
        :type local_stream_size: int
        :param target_base_path: the remote base path, eg, the bucket name.
        :type target_base_path: str
        :param target_name: the remote object name.
        :type target_name: str
        :return: True if no error occurred.
        :rtype: bool
        """
        ...

    @abstractmethod
    def _download(
        self,
        local_stream: BinaryIO,
        source_base_path: str,
        source_name: str,
        source_offset: int,
    ) -> bool:
        """Download and write data to a writable stream.

        :param local_stream: a writable stream.
        :type local_stream: BinaryIO
        :param source_base_path: the remote base path, eg, the bucket name.
        :type source_base_path: str
        :param source_name: the remote object name.
        :type source_name: str
        :param source_offset: where to start reading the remote data, defaults to 0
        :type source_offset: int, optional
        :return: True if no error occurred.
        :rtype: bool
        """
        ...

    @classmethod
    @abstractmethod
    def scheme(cls) -> str:
        """Return the scheme name, eg, 's3' or 'file'.

        :return: the scheme name.
        :rtype: str
        """
        pass

    @property
    def netloc(self) -> str:
        """Return the remote network address.

        :return: the remote network address.
        :rtype: str
        """
        return self._netloc

    @property
    @abstractmethod
    def is_valid(self) -> bool:
        """Check if this remote instance is valid and usable.

        :return: True if this instance is valid.
        :rtype: bool
        """
        pass


class S3Remote(BaseRemote):
    _HASH_FN_KEY_ = "__HASH_FN__"
    _DEFAULT_HASH_FN_ = "sha256"

    def __init__(
        self,
        endpoint: str,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        session_token: Optional[str] = None,
        secure_connection: bool = True,
        region: Optional[str] = None,
    ):
        """S3-compatible remote. Credential can be passed or retrieved from:
            * env var:
                ** access key: AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY, MINIO_ACCESS_KEY
                ** secret key: AWS_SECRET_ACCESS_KEY, AWS_SECRET_KEY, MINIO_SECRET_KEY
                ** session token: AWS_SESSION_TOKEN
            * config files:
                ** ~/.aws/credentials
                ** ~/[.]mc/config.json

        :param endpoint: the endpoint address
        :type endpoint: str
        :param access_key: optional user id, defaults to None
        :type access_key: Optional[str], optional
        :param secret_key: optional user password, defaults to None
        :type secret_key: Optional[str], optional
        :param session_token: optional session token, defaults to None
        :type session_token: Optional[str], optional
        :param secure_connection: should use a secure connection, defaults to True
        :type secure_connection: bool, optional
        :param region: optional region, defaults to None
        :type region: Optional[str], optional
        """
        super().__init__(endpoint)

        try:
            from minio import Minio

            from minio.credentials import (
                ChainedProvider,
                EnvAWSProvider,
                EnvMinioProvider,
                AWSConfigProvider,
                MinioClientConfigProvider,
            )

            self._client = Minio(
                endpoint=endpoint,
                access_key=access_key,
                secret_key=secret_key,
                session_token=session_token,
                secure=secure_connection,
                region=region,
                credentials=ChainedProvider(
                    [
                        # AWS_ACCESS_KEY_ID|AWS_ACCESS_KEY,
                        # AWS_SECRET_ACCESS_KEY|AWS_SECRET_KEY,
                        # AWS_SESSION_TOKEN
                        EnvAWSProvider(),
                        # MINIO_ACCESS_KEY, MINIO_SECRET_KEY
                        EnvMinioProvider(),
                        # ~/.aws/credentials
                        AWSConfigProvider(),
                        # ~/[.]mc/config.json
                        MinioClientConfigProvider(),
                    ]
                ),
            )
        except ModuleNotFoundError:  # pragma: no cover
            logger.error("S3 remote needs `minio` python package.")
            self._client = None

    @property
    def client(self):
        return self._client

    def _maybe_create_bucket(self, target_base_path: str):
        if not self._client.bucket_exists(target_base_path):  # type: ignore
            logger.info(
                f"Creating bucket '{target_base_path}' on S3 remote {self.netloc}."
            )
            self._client.make_bucket(target_base_path)  # type: ignore

    def _get_hash_fn(self, target_base_path: str) -> Any:
        if self.is_valid:
            try:
                self._maybe_create_bucket(target_base_path)
                tags = self._client.get_bucket_tags(target_base_path)  # type: ignore
                if tags is None:
                    from minio.commonconfig import Tags

                    tags = Tags()
                hash_fn_name = tags.get(self._HASH_FN_KEY_)

                # try-get
                if isinstance(hash_fn_name, str) and len(hash_fn_name) > 0:
                    try:
                        hash_fn = getattr(hashlib, hash_fn_name)
                        return hash_fn()
                    except AttributeError:
                        pass

                tags[self._HASH_FN_KEY_] = self._DEFAULT_HASH_FN_
                self._client.set_bucket_tags(target_base_path, tags)  # type: ignore

                hash_fn = getattr(hashlib, self._DEFAULT_HASH_FN_)
                return hash_fn()
            except Exception as exc:
                logger.debug(str(exc))
        return None

    def target_exists(self, target_base_path: str, target_name: str) -> bool:
        if self.is_valid:
            try:
                objlist = self._client.list_objects(  # type: ignore
                    target_base_path, prefix=target_name
                )
                _ = next(objlist)
                return True
            except StopIteration:
                return False
            except Exception as exc:
                logger.debug(str(exc))
        return False

    def _upload(
        self,
        local_stream: BinaryIO,
        local_stream_size: int,
        target_base_path: str,
        target_name: str,
    ) -> bool:
        if self.is_valid:
            try:
                self._maybe_create_bucket(target_base_path)
                self._client.put_object(  # type: ignore
                    bucket_name=target_base_path,
                    object_name=target_name,
                    data=local_stream,
                    length=local_stream_size,
                )
                return True
            except Exception as exc:
                logger.debug(str(exc))
                return False

        return False

    def _download(
        self,
        local_stream: BinaryIO,
        source_base_path: str,
        source_name: str,
        source_offset: int,
    ) -> bool:
        if self.is_valid:
            if not self._client.bucket_exists(source_base_path):  # type: ignore
                logger.debug(
                    f"Bucket '{source_base_path}' does not exist "
                    f"on S3 remote '{self.netloc}'."
                )
                return False

            response = None
            ok = False
            try:
                response = self._client.get_object(  # type: ignore
                    bucket_name=source_base_path,
                    object_name=source_name,
                    offset=source_offset,
                )
                for data in response.stream(amt=1024 * 1024):
                    local_stream.write(data)
                ok = True
            except Exception as exc:
                logger.debug(str(exc))
                return False
            finally:
                if response:
                    response.close()
                    response.release_conn()
                return ok

        return False

    @classmethod
    def scheme(cls) -> str:
        return "s3"

    @property
    def is_valid(self) -> bool:
        return self._client is not None


class FileRemote(BaseRemote):
    _PL_FOLDER_ = ".pl"
    _TAGS_FILE_ = "tags.json"
    _HASH_FN_KEY_ = "__HASH_FN__"
    _DEFAULT_HASH_FN_ = "sha256"

    def __init__(self, netloc: str):
        """Filesystem-based remote.

        :param netloc: the ip address, can be empty.
        :type netloc: str
        """
        if netloc == "localhost" or netloc == "127.0.0.1":
            netloc = ""
        super().__init__(netloc)

    def _maybe_create_root(self, target_base_path: Path):
        if not target_base_path.exists():
            logger.info(f"Creating folder tree '{target_base_path}'.")
            target_base_path.mkdir(parents=True, exist_ok=True)
        pldir = target_base_path / self._PL_FOLDER_
        if not pldir.is_dir():
            pldir.mkdir(parents=True, exist_ok=True)

    def _get_hash_fn(self, target_base_path: str) -> Any:
        if self.is_valid:
            try:
                import json

                target_root = self._make_file_path(target_base_path, "")
                self._maybe_create_root(target_root)

                tags = {}
                try:
                    with open(
                        target_root / self._PL_FOLDER_ / self._TAGS_FILE_, "r"
                    ) as jtags:
                        tags = json.load(jtags)
                except Exception:
                    pass

                hash_fn_name = tags.get(self._HASH_FN_KEY_)

                # try-get
                if isinstance(hash_fn_name, str) and len(hash_fn_name) > 0:
                    try:
                        hash_fn = getattr(hashlib, hash_fn_name)
                        return hash_fn()
                    except AttributeError:
                        pass

                tags[self._HASH_FN_KEY_] = self._DEFAULT_HASH_FN_
                with open(
                    target_root / self._PL_FOLDER_ / self._TAGS_FILE_, "w"
                ) as jtags:
                    json.dump(tags, jtags)

                hash_fn = getattr(hashlib, self._DEFAULT_HASH_FN_)
                return hash_fn()
            except Exception as exc:
                logger.debug(str(exc))
        return None

    def target_exists(self, target_base_path: str, target_name: str) -> bool:
        if self.is_valid:
            return self._make_file_path(target_base_path, target_name).exists()
        return False

    def _upload(
        self,
        local_stream: BinaryIO,
        local_stream_size: int,
        target_base_path: str,
        target_name: str,
    ) -> bool:
        if self.is_valid:
            try:
                target_full_path = self._make_file_path(target_base_path, target_name)
                self._maybe_create_root(target_full_path.parent)

                with target_full_path.open("wb") as target:
                    shutil.copyfileobj(local_stream, target)

                return True
            except Exception as exc:
                logger.debug(str(exc))
                return False

        return False

    def _download(
        self,
        local_stream: BinaryIO,
        source_base_path: str,
        source_name: str,
        source_offset: int,
    ) -> bool:
        if self.is_valid:
            try:
                source_full_path = self._make_file_path(source_base_path, source_name)
                if not source_full_path.is_file():
                    logger.debug(f"File '{source_full_path}' does not exist.")
                    return False

                with source_full_path.open("rb") as source:
                    source.seek(source_offset)
                    shutil.copyfileobj(source, local_stream)

                return True
            except Exception as exc:
                logger.debug(str(exc))
                return False

        return False

    def _make_file_path(self, file_path: str, file_name: str) -> Path:
        full_path = Path(file_path) / Path(file_name)
        if self.netloc:
            return Path(
                "{0}{0}{1}{0}{2}".format(os.path.sep, self.netloc, str(full_path))
            )
        elif isinstance(full_path, WindowsPath):
            return full_path
        else:
            return Path(os.path.sep + str(full_path))

    @classmethod
    def scheme(cls) -> str:
        return "file"

    @property
    def is_valid(self) -> bool:
        return True
