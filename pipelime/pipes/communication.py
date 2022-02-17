import json
from abc import ABC, abstractmethod
import os
from typing import Callable

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
        self._client.wait_for_bulletins()


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
        for _ in self._pubsub.listen():
            pass

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


class PiperCommunicationChannelFactory:
    """Factory for `PiperCommunicationChannel` objects"""

    CHANNEL_TYPE_VARNAME = "PIPELINE_PIPER_CHANNEL_TYPE"
    """Name of the environment variable with the channel type"""

    BULLETIN_BOARD = "BULLETIN"
    """Channel type env value for Choixe BulletinBoard"""
    REDIS = "REDIS"
    """Channel type env value for Redis"""

    _default_channel_cls = PiperCommunicationChannelBulletinBoard

    _cls_map = {
        BULLETIN_BOARD: PiperCommunicationChannelBulletinBoard,
        REDIS: PiperCommunicationChannelRedis,
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
