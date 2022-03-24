import json
import os
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Optional

import appdirs
import redis
from choixe.bulletins import Bulletin, BulletinBoard
from loguru import logger


class PiperCommunicationChannel(ABC):
    def __init__(self, token: str) -> None:
        """Generic Piper communication channel.

        Args:
            token (str): The token to use for the communication.
        """
        self._token = token

    @property
    def token(self) -> str:
        return self._token

    @abstractmethod
    def send(self, id: str, payload: any) -> bool:
        """Send a value to the communication channel.

        Args:
            id (str): sender id
            payload (any): payload to send

        Returns:
            bool: True if send was successful
        """
        raise NotImplementedError()

    @abstractmethod
    def register_callback(self, callback: Callable[[dict], None]) -> bool:
        """Register a callback to call every time a message is received"""
        raise NotImplementedError()

    @abstractmethod
    def listen(self) -> None:
        """Wait for new messages"""
        raise NotImplementedError()

    @abstractmethod
    def close(self) -> None:
        """Stop waiting for new messages"""
        raise NotImplementedError()


class PiperCommunicationChannelBulletinBoard(PiperCommunicationChannel):
    def __init__(self, token: str) -> None:
        """Create a new communication channel based on a bulletin board (choixe)
        It uses a MQTT client to send messages to the bulletin board which is a MQTT
        subscriber. This channel needs an active MQTT broker to work. If no broker is
        available, the channel will not work and an error message will be logged.

        Args:
            token (str): token to use for the communication
        """

        super().__init__(token)

        self._client = None

        try:
            self._client = BulletinBoard(session_id=token)
        except Exception as e:
            logger.error(f"{self.__class__.__name__} No Bulletin server!|{e}")

    @property
    def valid(self) -> bool:
        return self._client is not None

    def send(self, id: str, payload: any) -> bool:
        """Send a value to the communication channel.

        Args:
            id (str): sender id
            payload (any): payload to send

        Returns:
            bool: True if the value was sent, False otherwise
        """

        if self.valid:
            self._client.hang(
                Bulletin(
                    metadata={
                        "id": id,
                        "token": self._token,
                        "payload": payload,
                    }
                )
            )
            return True
        return False

    def register_callback(self, callback: Callable[[dict], None]) -> bool:
        def bulletin_helper(bulletin: Bulletin) -> None:
            callback(bulletin.metadata)

        if self.valid:
            self._client.register_callback(bulletin_helper)
            return True
        return False

    def listen(self) -> None:
        # BUG how do you gracefully stop a BulletinBoard?
        try:
            self._client.wait_for_bulletins()
        except:
            pass

    def close(self) -> None:
        # BUG how do you gracefully stop a BulletinBoard?
        try:
            self._client.close()
        except:
            pass


class PiperCommunicationChannelRedis(PiperCommunicationChannel):
    """`PiperCommunicationChannel` implementation for Redis backend.

    It uses a redis client to send/receive messages using redis pub/sub implementation.
    This channel needs an active redis instance. If no redis server is
    available, the channel will not work and an error message will be logged.
    """

    def __init__(self, token: str) -> None:
        """Constructor for `PiperCommunicationChannelRedis`

        Args:
            token (str): The token to use for the communication.
        """
        super().__init__(token)

        self._db = None
        self._thread = None

        try:
            self._db = redis.Redis()
            self._pubsub = self._db.pubsub()
        except Exception as e:
            logger.error(f"{self.__class__.__name__} No Redis server!|{e}")

    @property
    def valid(self) -> bool:
        return self._db is not None

    def send(self, id: str, payload: any) -> bool:
        if self.valid:
            data = {"id": id, "token": self._token, "payload": payload}
            msg = json.dumps(data).encode("utf-8")
            self._db.publish(self._token, msg)
            return True
        return False

    def listen(self) -> None:
        self._thread = self._pubsub.run_in_thread()

    def close(self) -> None:
        self._thread.stop()

    def register_callback(self, callback: Callable[[dict], None]) -> bool:

        # Callback helper that converts bytes to dictionary before call
        def redis_helper(msg: dict):
            json_string = msg["data"].decode("utf-8")
            data = json.loads(json_string)
            callback(data)

        if self.valid:
            self._pubsub.subscribe(**{self._token: redis_helper})
            return True
        return False


class PiperCommunicationChannelFS(PiperCommunicationChannel):
    """`PiperCommunicationChannel` implementation for Filesystem FIFO backend.

    It uses a Filesystem FIFO to send/receive messages. Only works with one producer
    and one consumer active at a time.
    """

    def __init__(self, token: str) -> None:
        """Constructor for `PiperCommunicationChannelFS`

        Args:
            token (str): The token to use for the communication.
        """
        super().__init__(token)
        self._file = Path(appdirs.user_state_dir("pipelime")) / f"{token}_fifo"
        self._file.parent.mkdir(parents=True, exist_ok=True)

        self._rfd = None
        with open(self._file, "ab"):
            pass

        self._cbs = []
        self._stop_flag = False

    def _try_write(self, data: Optional[dict]) -> bool:
        res = True
        try:
            with open(self._file, "ab") as fp:
                if data is not None:
                    bytes_ = json.dumps(data).encode("utf8")
                    fp.write(len(bytes_).to_bytes(4, byteorder="big", signed=True))
                    fp.write(bytes_)
                else:
                    fp.write(int(-1).to_bytes(4, byteorder="big", signed=True))
        except:
            res = False
        return res

    def _try_read(self) -> Optional[dict]:
        if self._rfd is None:
            self._rfd = open(self._file, "rb")

        data = None
        try:
            n = int.from_bytes(self._rfd.read(4), byteorder="big", signed=True)
            if n >= 0:
                data = json.loads(self._rfd.read(n).decode("utf8"))
            if n < 0:
                self._rfd.close()
        except:
            data = None
        return data

    def send(self, id: str, payload: any) -> bool:
        data = {"id": id, "token": self._token, "payload": payload}
        return self._try_write(data)

    def register_callback(self, callback: Callable[[dict], None]) -> bool:
        self._cbs.append(callback)
        return True

    def listen(self) -> None:
        while not self._stop_flag:
            data = self._try_read()

            if data is not None:
                for cb in self._cbs:
                    cb(data)

            time.sleep(0.001)

    def close(self) -> None:
        self._stop_flag = True
        self._try_write(None)

        while self._file.exists():
            self._file.unlink(missing_ok=True)
            time.sleep(0.5)


class PiperCommunicationChannelFactory:
    """Factory for `PiperCommunicationChannel` objects"""

    CHANNEL_TYPE_VARNAME = "PIPELIME_PIPER_CHANNEL_TYPE"
    """Name of the environment variable with the channel type"""

    BULLETIN_BOARD = "BULLETIN"
    """Channel type env value for Choixe BulletinBoard"""

    REDIS = "REDIS"
    """Channel type env value for Redis"""

    FILESYSTEM = "FILESYSTEM"
    """Channel type env value for FileSystem FIFO"""

    _default_channel_cls = PiperCommunicationChannelFS

    _cls_map = {
        BULLETIN_BOARD: PiperCommunicationChannelBulletinBoard,
        REDIS: PiperCommunicationChannelRedis,
        FILESYSTEM: PiperCommunicationChannelFS,
    }

    @classmethod
    def create_channel(cls, token: str) -> PiperCommunicationChannel:
        """Instantiates a `PiperCommunicationChannel` with a given token.
        The type of the returned channel can be controlled by setting an environment
        variable.

        Args:
            token (str): The token used for communication

        Returns:
            PiperCommunicationChannel: The instanced communication channel
        """
        channel_type = os.getenv(cls.CHANNEL_TYPE_VARNAME)
        channel_cls = cls._cls_map.get(channel_type, cls._default_channel_cls)
        return channel_cls(token)
