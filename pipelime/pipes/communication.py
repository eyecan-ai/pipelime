import json
from abc import ABC, abstractmethod
from typing import Callable

import redis
from choixe.bulletins import Bulletin, BulletinBoard
from loguru import logger


class PiperCommunicationChannel(ABC):
    def __init__(self, token: str) -> None:
        """Generics Piper communication channel.

        :param token: The token to use for the communication.
        :type token: str
        """
        self._token = token

    @property
    def token(self) -> str:
        return self._token

    @abstractmethod
    def send(self, id: str, payload: any) -> bool:
        """Send a value to the communication channel.

        :param id: sender id
        :type id: str
        :param payload: payload to send
        :type payload: any
        :return: True if send was successful
        :rtype: bool
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


class PiperCommunicationChannelFactory:
    @classmethod
    def create_channel(cls, token: str) -> PiperCommunicationChannel:
        # return PiperCommunicationChannelBulletinBoard(token)
        return PiperCommunicationChannelRedis(token)


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
    def __init__(self, token: str) -> None:
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
