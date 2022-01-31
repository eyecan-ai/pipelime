from abc import ABCMeta, abstractmethod
from pathlib import Path, WindowsPath
from typing import Any, Tuple, Union, Optional, BinaryIO
import hashlib
from urllib.parse import urlparse, unquote_plus, ParseResult

import os
import shutil

from loguru import logger


class RemoteRegister(ABCMeta):
    REMOTE_CLASSES = {}
    REMOTE_INSTANCES = {}

    def __init__(cls, name, bases, dct):
        cls.REMOTE_CLASSES[cls.scheme()] = cls  # type: ignore
        super().__init__(name, bases, dct)

    @classmethod
    def get_instance(cls, scheme: str, netloc: str, **kwargs):
        remote_instance = cls.REMOTE_INSTANCES.get((scheme, netloc))
        if remote_instance is None:
            remote_class = cls.REMOTE_CLASSES.get(scheme)
            if remote_class is not None:
                remote_instance = remote_class(netloc, **kwargs)
                cls.REMOTE_INSTANCES[(scheme, netloc)] = remote_instance
            else:
                logger.warning(f"Unknown remote scheme {scheme}.")
        return remote_instance


def create_remote(scheme: str, netloc: str, **kwargs):
    return RemoteRegister.get_instance(scheme, netloc, **kwargs)


def create_remote_from_url(
    url: str,
) -> Tuple[Optional["BaseRemote"], Optional[str], Optional[str]]:
    parsed_url = urlparse(url)

    if len(parsed_url.path) > 1:
        file_full_path = Path(unquote_plus(parsed_url.path)[1:])
        file_path, file_name = file_full_path.parent, file_full_path.name
    else:
        file_path, file_name = None, None

    return (
        RemoteRegister.get_instance(parsed_url.scheme, parsed_url.netloc),
        str(file_path),
        str(file_name),
    )


class BaseRemote(metaclass=RemoteRegister):  # type: ignore
    def __init__(self, netloc: str):
        self._netloc = netloc

    def upload_file(
        self, file_path: Union[Path, str], target_path: str
    ) -> Optional[str]:
        file_path = Path(file_path)
        file_size = file_path.stat().st_size
        with open(file_path, "rb") as file_data:
            return self.upload_stream(
                file_data, file_size, target_path, "".join(file_path.suffixes)
            )

    def upload_stream(
        self,
        stream: BinaryIO,
        stream_size: int,
        target_path: str,
        target_suffix: str,
    ) -> Optional[str]:
        hash_name = self._compute_hash(stream, self._get_hash_fn(target_path))
        target_name = hash_name + target_suffix
        if self._upload(stream, stream_size, target_path, target_name):
            return self._make_url(f"{target_path}/{target_name}")
        return None

    def download_file(
        self,
        file_path: Union[Path, str],
        source_path: str,
        source_name: str,
    ) -> bool:
        file_path = Path(file_path)
        if file_path.suffixes != Path(source_name).suffixes:
            file_path = file_path.with_suffix(
                "".join([file_path.suffix] + Path(source_name).suffixes)
            )
        file_path.parent.mkdir(parents=True, exist_ok=True)

        part_file = file_path.with_suffix(file_path.suffix + ".part")

        offset: int = 0
        try:
            offset = part_file.stat().st_size
        except IOError:
            pass

        ok = False
        with open(part_file, "ab") as part_stream:
            ok = self.download_stream(part_stream, source_path, source_name, offset)

        if ok:
            file_path.unlink(missing_ok=True)
            part_file.rename(file_path)

        return ok

    def download_stream(
        self,
        stream: BinaryIO,
        source_path: str,
        source_name: str,
        source_offset: int = 0,
    ) -> bool:
        return self._download(stream, source_path, source_name, source_offset)

    def _compute_hash(self, stream: BinaryIO, hash_fn: Any = None) -> str:
        if hash_fn is None:
            hash_fn = hashlib.sha256()
        b = bytearray(1024 * 1024)
        mv = memoryview(b)
        fpos = stream.tell()
        for n in iter(lambda: stream.readinto(mv), 0):  # type: ignore
            hash_fn.update(mv[:n])
        stream.seek(fpos)
        return hash_fn.hexdigest()

    def _make_url(self, target_path: str):
        return (
            ParseResult(
                scheme=self.scheme(),
                netloc=self.netloc,
                path=target_path,
                params="",
                query="",
                fragment="",
            )
            .geturl()
            .replace("\\", "/")
        )

    @abstractmethod
    def _get_hash_fn(self, target_path: str) -> Any:
        ...

    @abstractmethod
    def _upload(
        self,
        source: BinaryIO,
        source_size: int,
        target_path: str,
        target_name: str,
    ) -> bool:
        ...

    @abstractmethod
    def _download(
        self,
        target: BinaryIO,
        source_path: str,
        source_name: str,
        source_offset: int,
    ) -> bool:
        ...

    @classmethod
    @abstractmethod
    def scheme(cls) -> str:
        pass

    @property
    def netloc(self) -> str:
        return self._netloc

    @property
    @abstractmethod
    def is_valid(self) -> bool:
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
                # credentials=ChainedProvider(
                #     [
                #         # AWS_ACCESS_KEY_ID|AWS_ACCESS_KEY,
                #         # AWS_SECRET_ACCESS_KEY|AWS_SECRET_KEY,
                #         # AWS_SESSION_TOKEN
                #         EnvAWSProvider(),
                #         # MINIO_ACCESS_KEY, MINIO_SECRET_KEY
                #         EnvMinioProvider(),
                #         # ~/.aws/credentials
                #         AWSConfigProvider(),
                #         # ~/[.]mc/config.json
                #         MinioClientConfigProvider(),
                #     ]
                # ),
            )
        except ModuleNotFoundError:  # pragma: no cover
            logger.error("S3 remote needs `minio` python package.")
            self._client = None

    def _maybe_create_bucket(self, target_path: str):
        if not self._client.bucket_exists(target_path):  # type: ignore
            logger.info(f"Creating bucket {target_path} on S3 remote {self.netloc}.")
            self._client.make_bucket(target_path)  # type: ignore

    def _get_hash_fn(self, target_path: str) -> Any:
        if self.is_valid:
            try:
                self._maybe_create_bucket(target_path)
                tags = self._client.get_bucket_tags(target_path)  # type: ignore
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
                self._client.set_bucket_tags(target_path, tags)  # type: ignore

                hash_fn = getattr(hashlib, self._DEFAULT_HASH_FN_)
                return hash_fn()
            except Exception as exc:
                logger.warning(str(exc))
        return None

    def _upload(
        self,
        source: BinaryIO,
        source_size: int,
        target_path: str,
        target_name: str,
    ) -> bool:
        if self.is_valid:
            try:
                self._maybe_create_bucket(target_path)
                self._client.put_object(  # type: ignore
                    bucket_name=target_path,
                    object_name=target_name,
                    data=source,
                    length=source_size,
                )
                return True
            except Exception as exc:
                logger.warning(str(exc))
                return False

        return False

    def _download(
        self,
        target: BinaryIO,
        source_path: str,
        source_name: str,
        source_offset: int,
    ) -> bool:
        if self.is_valid:
            if not self._client.bucket_exists(source_path):  # type: ignore
                logger.warning(
                    f"Bucket {source_path} does not exist "
                    f"on S3 remote {self.netloc}."
                )
                return False

            response = None
            ok = False
            try:
                response = self._client.get_object(  # type: ignore
                    bucket_name=source_path,
                    object_name=source_name,
                    offset=source_offset,
                )
                for data in response.stream(amt=1024 * 1024):
                    target.write(data)
                ok = True
            except Exception as exc:
                logger.warning(str(exc))
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
    _HASH_FN_KEY_ = "__HASH_FN__"
    _DEFAULT_HASH_FN_ = "sha256"

    def __init__(self, netloc: str):
        if netloc == "localhost" or netloc == "127.0.0.1":
            netloc = ""
        super().__init__(netloc)

    def _maybe_create_root(self, target_path: Path):
        if not target_path.exists():
            logger.info(f"Creating folder tree {target_path}.")
            target_path.mkdir(parents=True, exist_ok=True)
        pldir = target_path / self._PL_FOLDER_
        if not pldir.is_dir():
            pldir.mkdir(parents=True, exist_ok=True)

    def _get_hash_fn(self, target_path: str) -> Any:
        if self.is_valid:
            try:
                import json

                full_target_path = self._make_file_path(target_path, "")
                self._maybe_create_root(full_target_path)

                tags = {}
                try:
                    with open(
                        full_target_path / self._PL_FOLDER_ / "tags.json", "r"
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
                    full_target_path / self._PL_FOLDER_ / "tags.json", "w"
                ) as jtags:
                    json.dump(tags, jtags)

                hash_fn = getattr(hashlib, self._DEFAULT_HASH_FN_)
                return hash_fn()
            except Exception as exc:
                logger.warning(str(exc))
        return None

    def _upload(
        self,
        source: BinaryIO,
        source_size: int,
        target_path: str,
        target_name: str,
    ) -> bool:
        if self.is_valid:
            try:
                full_target_path = self._make_file_path(target_path, target_name)
                self._maybe_create_root(full_target_path.parent)

                with full_target_path.open("wb") as target:
                    shutil.copyfileobj(source, target)

                return True
            except Exception as exc:
                logger.warning(str(exc))
                return False

        return False

    def _download(
        self,
        target: BinaryIO,
        source_path: str,
        source_name: str,
        source_offset: int,
    ) -> bool:
        if self.is_valid:
            try:
                full_source_path = self._make_file_path(source_path, source_name)
                if not full_source_path.exists():
                    logger.warning(f"File {full_source_path} does not exist.")
                    return False

                with full_source_path.open("rb") as source:
                    source.seek(source_offset)
                    shutil.copyfileobj(source, target)

                return True
            except Exception as exc:
                logger.warning(str(exc))
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
