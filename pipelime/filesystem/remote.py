from abc import ABC, abstractmethod
from pathlib import Path
from io import RawIOBase, BufferedIOBase
from typing import Union, Optional

from loguru import logger


class BaseRemote(ABC):
    def __init__(self, hash_gen=None):
        import hashlib

        self._hash_gen = hashlib.sha256 if hash_gen is None else hash_gen

    def upload_file(
        self, file_path: Union[Path, str], target_prefix: str
    ) -> Optional[str]:
        file_path = Path(file_path)
        file_size = file_path.stat().st_size
        with open(file_path, "rb") as file_data:
            return self.upload_stream(
                file_data, file_size, target_prefix, "".join(file_path.suffixes)
            )

    def upload_stream(
        self,
        stream: Union[RawIOBase, BufferedIOBase],
        stream_size: int,
        target_prefix: str,
        target_suffix: str,
    ) -> Optional[str]:
        hash_name = self._compute_hash(stream)
        target_name = hash_name + target_suffix
        if self._upload(stream, stream_size, target_prefix, target_name):
            return self._make_url(target_prefix, target_name)
        return None

    def download_file(
        self,
        file_path: Union[Path, str],
        source_prefix: str,
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
            ok = self.download_stream(part_stream, source_prefix, source_name, offset)

        if ok:
            file_path.unlink(missing_ok=True)
            part_file.rename(file_path)

        return ok

    def download_stream(
        self,
        stream: Union[RawIOBase, BufferedIOBase],
        source_prefix: str,
        source_name: str,
        source_offset: int = 0,
    ) -> bool:
        return self._download(stream, source_prefix, source_name, source_offset)

    def _compute_hash(self, stream: Union[RawIOBase, BufferedIOBase]) -> str:
        h = self._hash_gen()
        b = bytearray(1024 * 1024)
        mv = memoryview(b)
        fpos = stream.tell()
        for n in iter(lambda: stream.readinto(mv), 0):
            h.update(mv[:n])
        stream.seek(fpos)
        return h.hexdigest()

    def _make_url(self, target_prefix: str, target_name: str):
        return (
            f"{self.protocol_code}://{self.endpoint_url}/{target_prefix}/{target_name}"
        )

    @abstractmethod
    def _upload(
        self,
        source: Union[RawIOBase, BufferedIOBase],
        source_size: int,
        target_prefix: str,
        target_name: str,
    ) -> bool:
        ...

    def _download(
        self,
        target: Union[RawIOBase, BufferedIOBase],
        source_prefix: str,
        source_name: str,
        source_offset: int,
    ) -> bool:
        ...

    @property
    @abstractmethod
    def protocol_code(self) -> str:
        pass

    @property
    @abstractmethod
    def endpoint_url(self) -> str:
        pass


class S3Remote(BaseRemote):
    def __init__(
        self,
        endpoint: str,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        session_token: Optional[str] = None,
        secure_connection: bool = True,
        region: Optional[str] = None,
        hash_gen=None,
    ):
        try:
            from minio import Minio
            from minio.credentials import (
                ChainedProvider,
                # MINIO_ACCESS_KEY, MINIO_SECRET_KEY
                EnvMinioProvider,
                # AWS_ACCESS_KEY_ID|AWS_ACCESS_KEY,
                # AWS_SECRET_ACCESS_KEY|AWS_SECRET_KEY,
                # AWS_SESSION_TOKEN
                EnvAWSProvider,
                # ~/[.]mc/config.json
                MinioClientConfigProvider,
                # ~/.aws/credentials
                AWSConfigProvider,
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
                        EnvMinioProvider(),
                        EnvAWSProvider(),
                        MinioClientConfigProvider(),
                        AWSConfigProvider(),
                    ]
                ),
            )
            self._endpoint = endpoint
        except ModuleNotFoundError:  # pragma: no cover
            logger.error("S3 remote needs `minio` python package.")  # pragma: no cover

    def _upload(
        self,
        source: Union[RawIOBase, BufferedIOBase],
        source_size: int,
        target_prefix: str,
        target_name: str,
    ) -> bool:
        try:
            if not self._client.bucket_exists(target_prefix):
                logger.info(
                    f"Creating bucket {target_prefix} on S3 remote {self.endpoint_url}"
                )
                self._client.make_bucket(target_prefix)
            self._client.put_object(
                bucket_name=target_prefix,
                object_name=target_name,
                data=source,
                length=source_size,
            )
        except Exception as exc:
            logger.warning(str(exc))
            return False

        return True

    def _download(
        self,
        target: Union[RawIOBase, BufferedIOBase],
        source_prefix: str,
        source_name: str,
        source_offset: int,
    ) -> bool:
        if not self._client.bucket_exists(source_prefix):
            logger.info(
                f"Bucket {source_prefix} does not exist "
                f"on S3 remote {self.endpoint_url}"
            )
            return False

        response = None
        try:
            response = self._client.get_object(
                bucket_name=source_prefix, object_name=source_name, offset=source_offset
            )
            for data in response.stream(amt=1024 * 1024):
                target.write(data)
        finally:
            if response:
                response.close()
                response.release_conn()
                return True
        return False

    @property
    def protocol_code(self) -> str:
        return "s3"

    @property
    def endpoint_url(self) -> str:
        return self._endpoint
