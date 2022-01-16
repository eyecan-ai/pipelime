from typing import Optional, Sequence, Union
import click
import yaml
from yaml.scanner import ScannerError
import subprocess
from loguru import logger
import inspect
import uuid
from pyarrow import plasma
from choixe.bulletins import BulletinBoard, Bulletin


class PiperNamespace:
    PIPER_PREFIX = "piper_"
    PRIVATE_ARGUMENT_PREFIX = "---"
    PRIVATE_OPTION_PREFIX = "_"
    NAME_INPUTS = "inputs"
    NAME_OUTPUTS = "outputs"
    NAME_TOKEN = "token"
    NAME_INFO = "info"

    OPTION_NAME_INPUTS = f"{PRIVATE_OPTION_PREFIX}{PIPER_PREFIX}{NAME_INPUTS}"
    OPTION_NAME_OUTPUTS = f"{PRIVATE_OPTION_PREFIX}{PIPER_PREFIX}{NAME_OUTPUTS}"
    OPTION_NAME_TOKEN = f"{PRIVATE_OPTION_PREFIX}{PIPER_PREFIX}{NAME_TOKEN}"
    OPTION_NAME_INFO = f"{PRIVATE_OPTION_PREFIX}{PIPER_PREFIX}{NAME_INFO}"
    ARGUMENT_NAME_INPUTS = f"{PRIVATE_ARGUMENT_PREFIX}{PIPER_PREFIX}{NAME_INPUTS}"
    ARGUMENT_NAME_OUTPUTS = f"{PRIVATE_ARGUMENT_PREFIX}{PIPER_PREFIX}{NAME_OUTPUTS}"
    ARGUMENT_NAME_TOKEN = f"{PRIVATE_ARGUMENT_PREFIX}{PIPER_PREFIX}{NAME_TOKEN}"
    ARGUMENT_NAME_INFO = f"{PRIVATE_ARGUMENT_PREFIX}{PIPER_PREFIX}{NAME_INFO}"


class Piper:
    def __init__(self, **kwargs):
        self._inputs = kwargs.get(PiperNamespace.OPTION_NAME_INPUTS, [])
        self._outputs = kwargs.get(PiperNamespace.OPTION_NAME_OUTPUTS, [])
        self._token = kwargs.get(PiperNamespace.OPTION_NAME_TOKEN, "")
        self._token = self._token if len(self._token) > 0 else None

        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        filename = module.__file__
        self._caller_name = f"{filename}:{module.__name__}:{frame.function}"
        self._unique_identifier = str(uuid.uuid1())
        self._id = f"{self._caller_name}:{self._unique_identifier}"

        if self.is_active():
            logger.debug(f"New Piper created from: {self._id}")
            logger.debug(f"\tPiper inputs: {self._inputs}")
            logger.debug(f"\tPiper outputs: {self._outputs}")
            logger.debug(f"\tPiper token: {self._token}")

            self._client = BulletinBoard(self._token)

    def is_active(self):
        return self._token is not None

    def log_value(self, key: str, value: any):
        if self.is_active():
            self._client.hang(
                Bulletin(
                    metadata={
                        "id": self._id,
                        "token": self._token,
                        "payload": {key: value},
                    }
                )
            )
            logger.debug(f"{self._id}|{key}:{value}|TOKEN[{self._token}]")

    @staticmethod
    def piper_info_callback(
        ctx: click.core.Context, param: click.core.Option, value: bool
    ):
        if value:
            click.echo(ctx.command.to_info_dict(ctx))
            ctx.exit()

    @staticmethod
    def add_piper_options(
        inputs: Optional[Sequence[str]] = None,
        outputs: Optional[Sequence[str]] = None,
    ):
        def _add_options(func):
            func = click.option(
                PiperNamespace.ARGUMENT_NAME_TOKEN, default="", hidden=True
            )(func)
            func = click.option(
                PiperNamespace.ARGUMENT_NAME_INPUTS, default=inputs, hidden=True
            )(func)
            func = click.option(
                PiperNamespace.ARGUMENT_NAME_OUTPUTS, default=outputs, hidden=True
            )(func)
            func = click.option(
                PiperNamespace.ARGUMENT_NAME_INFO,
                is_flag=True,
                is_eager=True,
                expose_value=False,
                callback=Piper.piper_info_callback,
                hidden=True,
            )(func)
            return func

        return _add_options

    @classmethod
    def piper_command_raw_info(cls, command: str) -> Union[None, dict]:

        command += f" {PiperNamespace.ARGUMENT_NAME_INFO}"

        info = None
        try:
            pipe = subprocess.Popen(
                command.split(" "),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE,
            )
            info = yaml.safe_load(pipe.stdout)
        except ScannerError as e:
            # print(e)
            info = None
        return info

    @classmethod
    def piper_command_description(cls, command: str):

        raw_info = cls.piper_command_raw_info(command)
        if raw_info is None:
            raise RuntimeError(f"Command '{command}' is not a piper!")

        commands_map = {x["name"]: x for x in raw_info["params"]}

        piper_inputs = commands_map[PiperNamespace.OPTION_NAME_INPUTS]
        piper_outputs = commands_map[PiperNamespace.OPTION_NAME_OUTPUTS]
        piper_token = commands_map[PiperNamespace.OPTION_NAME_TOKEN]

        inputs_list = piper_inputs["default"]
        outputs_list = piper_outputs["default"]

        description = {PiperNamespace.NAME_INPUTS: {}, PiperNamespace.NAME_OUTPUTS: {}}
        for i in inputs_list:
            assert i in commands_map, f"{i} is not a valid input"
            description["inputs"][i] = commands_map[i]

        for o in outputs_list:
            assert o in commands_map, f"{o} is not a valid output"
            description["outputs"][o] = commands_map[o]

        assert PiperNamespace.OPTION_NAME_TOKEN in commands_map, "token is not present"

        return description
