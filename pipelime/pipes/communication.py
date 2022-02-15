from abc import ABC, abstractmethod
from typing import Callable
from choixe.bulletins import BulletinBoard, Bulletin
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
    def register_callback(self, callback: Callable) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def listen(self) -> None:
        raise NotImplementedError()


class PiperCommunicationChannelFactory:
    @classmethod
    def create_channel(cls, token: str) -> PiperCommunicationChannel:
        return PiperCommunicationChannelBulletinBoard(token)


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

    def register_callback(self, callback: Callable) -> bool:
        def bulletin_helper(bulletin: Bulletin) -> None:
            callback(bulletin.metadata)

        if self.valid:
            self._client.register_callback(bulletin_helper)
            return True
        return False

    def listen(self) -> None:
        self._client.wait_for_bulletins()
