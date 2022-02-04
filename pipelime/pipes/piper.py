import functools
from pipelime.pipes.communication import PiperCommunicationChannelFactory
from typing import Callable, Optional, Sequence, Union
from yaml.scanner import ScannerError
from loguru import logger
import subprocess
import inspect
import click
import yaml
import uuid


class PiperNamespace:
    """Namespace constants for Piper ecosystem."""

    PIPER_PREFIX = "piper_"
    PRIVATE_ARGUMENT_PREFIX = "---"
    PRIVATE_OPTION_PREFIX = "_"
    NAME_INPUTS = "inputs"
    NAME_OUTPUTS = "outputs"
    NAME_TOKEN = "token"
    NAME_INFO = "info"
    NAME_ARGS = "args"
    COMMAND_KWARGS_NAME = "piper_kwargs"

    """ Names of kwargs variable """
    OPTION_NAME_INPUTS = f"{PRIVATE_OPTION_PREFIX}{PIPER_PREFIX}{NAME_INPUTS}"
    OPTION_NAME_OUTPUTS = f"{PRIVATE_OPTION_PREFIX}{PIPER_PREFIX}{NAME_OUTPUTS}"
    OPTION_NAME_TOKEN = f"{PRIVATE_OPTION_PREFIX}{PIPER_PREFIX}{NAME_TOKEN}"
    OPTION_NAME_INFO = f"{PRIVATE_OPTION_PREFIX}{PIPER_PREFIX}{NAME_INFO}"

    """ Names of click arguments """
    ARGUMENT_NAME_INPUTS = f"{PRIVATE_ARGUMENT_PREFIX}{PIPER_PREFIX}{NAME_INPUTS}"
    ARGUMENT_NAME_OUTPUTS = f"{PRIVATE_ARGUMENT_PREFIX}{PIPER_PREFIX}{NAME_OUTPUTS}"
    ARGUMENT_NAME_TOKEN = f"{PRIVATE_ARGUMENT_PREFIX}{PIPER_PREFIX}{NAME_TOKEN}"
    ARGUMENT_NAME_INFO = f"{PRIVATE_ARGUMENT_PREFIX}{PIPER_PREFIX}{NAME_INFO}"


class PiperCommandSingleton(type):
    _instance = None

    def __call__(cls, *args, **kwargs):
        if PiperCommandSingleton._instance is None:

            # Search the piper kwarg in the kwargs of the click command and add it to
            # the kwargs
            parent_variables = inspect.currentframe().f_back.f_locals
            if PiperNamespace.COMMAND_KWARGS_NAME in parent_variables:
                kwargs.update(parent_variables[PiperNamespace.COMMAND_KWARGS_NAME])
            else:
                raise ValueError(
                    f"{PiperNamespace.COMMAND_KWARGS_NAME} not found among command kwargs"
                )

            # Builds the caller name based on Module/Filename/Function path and add
            # it to the kwargs
            frame = inspect.stack()[1]
            module = inspect.getmodule(frame[0])
            filename = module.__file__
            caller_name = f"{filename}:{module.__name__}:{frame.function}"
            kwargs.update({"caller_name": caller_name})

            # Creates the PiperCommand instance
            PiperCommandSingleton._instance = super().__call__(*args, **kwargs)

        return PiperCommandSingleton._instance

    @classmethod
    def destroy(cls):
        PiperCommandSingleton._instance = None


class PiperCommand(metaclass=PiperCommandSingleton):
    def __init__(self, **kwargs) -> None:
        """Creates a new PiperCommand instance.

        Raises:
            RuntimeError: If the token is not provided.
        """

        # Extract default values from the kwargs
        self._caller_name = kwargs.get("caller_name", "Unknown")
        self._inputs = kwargs.get(PiperNamespace.OPTION_NAME_INPUTS, [])
        self._outputs = kwargs.get(PiperNamespace.OPTION_NAME_OUTPUTS, [])
        self._token = kwargs.get(PiperNamespace.OPTION_NAME_TOKEN, "")
        self._token = self._token if len(self._token) > 0 else None

        # progress callbacks list
        self._progress_callbacks = []

        # Token check, if not provided, the command is disabled
        self._active = self._token is not None

        if self._active:
            # Builds an unique id for the command, multiple instances of same command
            # are allowed, for this reason an unique id is required
            self._unique_identifier = str(uuid.uuid1())
            self._id = f"{self._caller_name}:{self._unique_identifier}"

            # Creates the communication channel among commands
            self._channel = PiperCommunicationChannelFactory.create_channel(self._token)

            # Logs the command creation
            logger.debug(f"{self._log_header}New Piper created from: {self._id}")
            logger.debug(f"{self._log_header}\tPiper inputs: {self._inputs}")
            logger.debug(f"{self._log_header}\tPiper outputs: {self._outputs}")
            logger.debug(f"{self._log_header}\tPiper token: {self._token}")

    def _progress_callback(self, chunk_index: int, total_chunks: int, payload: dict):
        self.log(
            "_progress",
            {
                "chunk_index": chunk_index,
                "total_chunks": total_chunks,
                "progress_data": payload,
            },
        )

    def generate_progress_callback(
        self,
        chunk_index: int = 0,
        total_chunks: int = 1,
    ) -> Callable[[dict], None]:
        """Generates a progress callback function to send back to the caller. The callback
        should be called every time the external progress is updated. Internally the callback
        will log the progress (also in the communication channel).

        Args:
            chunk_index (int, optional): index for multiple user tasks. Defaults to 0.
            total_chunks (int, optional): total number of chunks. Defaults to 1.

        Returns:
            Callable[[dict], None]: the callback function
        """
        callback = functools.partial(self._progress_callback, chunk_index, total_chunks)
        self._progress_callbacks.append(callback)
        return callback

    def clear_progress_callbacks(self):
        self._progress_callbacks = []

    @property
    def active(self) -> bool:
        return self._active

    @property
    def _log_header(self) -> str:
        return f"{self._id}|"

    def log(self, key: str, value: any):
        """Logs a key/value pair into the communication channel.

        Args:
            key (str): The key to log.
            value (any): The value to log. Can be any picklable object.
        """
        if self.active:
            logger.debug(f"{self._log_header}Logging {key}={value}")
            self._channel.send(self._id, {key: value})

    def __del__(self):
        self.destroy()

    def destroy(self):
        PiperCommandSingleton.destroy()


class Piper:
    @staticmethod
    def _piper_info_callback(
        ctx: click.core.Context, param: click.core.Option, value: bool
    ):
        """Callback for the click eager corresponding option. If the eager option is
        set to True, the command will exit printing the Piper info into the pipe.

        Args:
            ctx (click.core.Context): The click context.
            param (click.core.Option): The click option.
            value (bool): The value of the eager option.
        """
        if value:
            click.echo(ctx.command.to_info_dict(ctx))
            ctx.exit()

    @staticmethod
    def piper_command_options(
        inputs: Optional[Sequence[str]] = None,
        outputs: Optional[Sequence[str]] = None,
    ):
        """This is the special decorator for the Piper command. It is used to add hidden
        options to the command used to manager the Piper ecosystem.

        Args:
            inputs (Optional[Sequence[str]], optional): List of click command options
            treated as inputs. Defaults to None.
            outputs (Optional[Sequence[str]], optional): List of click command options
            treated as output . Defaults to None.
        """

        def _add_options(func):

            # Add the token option
            func = click.option(
                PiperNamespace.ARGUMENT_NAME_TOKEN, default="", hidden=True
            )(func)

            # Add the inputs options
            func = click.option(
                PiperNamespace.ARGUMENT_NAME_INPUTS, default=inputs, hidden=True
            )(func)

            # Add the output options
            func = click.option(
                PiperNamespace.ARGUMENT_NAME_OUTPUTS, default=outputs, hidden=True
            )(func)

            # Add the info option. This is the eager option, if set to True, the
            # execution will exit printing the Piper info into the pipe.
            func = click.option(
                PiperNamespace.ARGUMENT_NAME_INFO,
                is_flag=True,
                is_eager=True,
                expose_value=False,
                callback=Piper._piper_info_callback,
                hidden=True,
            )(func)

            return func

        return _add_options

    @classmethod
    def piper_command_raw_info(cls, command: str) -> Union[None, dict]:
        """Retrieves the Piper info from a generic bash command. If the bash command
        is a valid Piper command, the Piper info will be returned as a dict. If the
        bash command is not a valid Piper command, None will be returned.

        Args:
            command (str): The bash command to execute. The command should be the raw bash
            command, without any arguments provided. Automatically the eager option will
            be added to the command to force the Piper to manifest itself.

        Returns:
            Union[None, dict]: The Piper info as a dict or None if the bash command
        """

        # Append the piper eager option to the command
        command += f" {PiperNamespace.ARGUMENT_NAME_INFO}"

        info = None
        try:

            # Execute the command and retrieve the output into the PIPE
            pipe = subprocess.Popen(
                command.split(" "),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE,
            )

            # Reader the stdout to retrieve the Piper info if any
            info = yaml.safe_load(pipe.stdout)
        except ScannerError as e:
            logger.error(f"{command} is not a valid Piper command! {str(e)}")
            info = None
        return info

    @classmethod
    def piper_command_description(cls, command: str) -> dict:
        """Retrieves the Piper structured description from a generic bash command.
        If the bash command is a valid Piper command, the Piper description will be returned
        as a dict. If the bash command is not a valid Piper command, None will be returned.

        Args:
            command (str): The bash command to execute. The command should be the raw bash
            command, without any arguments provided. Automatically the eager option will
            be added to the command to force the Piper to manifest itself.

        Raises:
            RuntimeError: If the Piper command is not a valid Piper command.

        Returns:
            dict: The Piper description as a dict or None if the bash command is not a
            valid Piper command.
        """

        raw_info = cls.piper_command_raw_info(command)
        if raw_info is None:
            raise RuntimeError(f"Command '{command}' is not a piper!")

        commands_map = {x["name"]: x for x in raw_info["params"]}

        # Checks for the mandatory piper options
        assert PiperNamespace.OPTION_NAME_INPUTS in commands_map, "No inputs!"
        assert PiperNamespace.OPTION_NAME_OUTPUTS in commands_map, "No outputs!"
        assert PiperNamespace.OPTION_NAME_TOKEN in commands_map, "token is not present"
        assert PiperNamespace.OPTION_NAME_INFO in commands_map, "info is not present"

        piper_inputs = commands_map[PiperNamespace.OPTION_NAME_INPUTS]
        piper_outputs = commands_map[PiperNamespace.OPTION_NAME_OUTPUTS]

        # Remove the piper options from the commands map
        del commands_map[PiperNamespace.OPTION_NAME_INPUTS]
        del commands_map[PiperNamespace.OPTION_NAME_OUTPUTS]
        del commands_map[PiperNamespace.OPTION_NAME_TOKEN]
        del commands_map[PiperNamespace.OPTION_NAME_INFO]

        # Retrieves inputs/outputs fields
        inputs_list = piper_inputs["default"]
        outputs_list = piper_outputs["default"]

        # initialize the description
        description = {
            PiperNamespace.NAME_INPUTS: {},
            PiperNamespace.NAME_OUTPUTS: {},
            PiperNamespace.NAME_ARGS: {},
        }

        # For each inputs check if a corresponding click command option is present
        for i in inputs_list:
            assert i in commands_map, f"{i} is not a valid input"
            description[PiperNamespace.NAME_INPUTS][i] = commands_map[i]
            del commands_map[i]

        # For each outputs check if a corresponding click command option is present
        for o in outputs_list:
            assert o in commands_map, f"{o} is not a valid output"
            description[PiperNamespace.NAME_OUTPUTS][o] = commands_map[o]
            del commands_map[o]

        # Adds remaining options as generic arguments
        description[PiperNamespace.NAME_ARGS] = commands_map

        return description

    @classmethod
    def piper_info_argument(cls):
        return PiperNamespace.ARGUMENT_NAME_INFO

    @classmethod
    def piper_token_argument(cls):
        return PiperNamespace.ARGUMENT_NAME_TOKEN
