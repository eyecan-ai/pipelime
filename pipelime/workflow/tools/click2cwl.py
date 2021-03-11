import importlib.util
from typing import Union, Sequence
from pathlib import Path
import click
from click.decorators import command
import yaml


class Click2Cwl(object):

    @classmethod
    def load_click(cls, script: str) -> Union[click.Command, None]:
        """ Loads a click.Command from file, it assumes that
        the script contains only one Command

        :param script: the script filename
        :type script: str
        :return: the click command if present, else None
        :rtype: Union[click.Command, None]
        """

        script = Path(script)
        spec = importlib.util.spec_from_file_location(script.stem, script)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        for x in dir(module):
            x = getattr(module, x)
            if isinstance(x, click.core.Command):
                return x

        return None

    @classmethod
    def cwl_template(cls) -> dict:
        """ Returns a template for a cwl CommandLineTool

        :return: the cwl file template
        :rtype: dict
        """

        cwl = dict()
        cwl['cwlVersion'] = 'v1.0'
        cwl['class'] = 'CommandLineTool'
        cwl['doc'] = ''
        cwl['baseCommand'] = ''
        cwl['inputs'] = dict()
        cwl['outputs'] = dict()
        return cwl

    @classmethod
    def fill_command(cls, cwl: dict, cmd: click.Command, commands: list) -> dict:
        """ Fills a cwl with the command informations

        :param cwl: the cwl file
        :type cwl: dict
        :param cmd: the click command
        :type cmd: click.Command
        :param cli_path: the mandatory arguments to the base command
        :type cli_path: list
        :param base_command: the base command to be called, defaults to 'esurface'
        :type base_command: str, optional
        :return: the filled cwl file
        :rtype: dict
        """

        cwl['doc'] = cmd.help
        cwl['baseCommand'] = commands
        return cwl

    @classmethod
    def fill_inputs(cls, cwl: dict, cmd: click.Command) -> dict:
        """ Fills a cwl with the input informations

        :param cwl: the cwl file
        :type cwl: dict
        :param cmd: the click command
        :type cmd: click.Command
        :return: the filled cwl file
        :rtype: dict
        """

        for param in cmd.params:
            param: click.Option
            cwl_param = dict()
            cwl_param['doc'] = param.help
            cwl_type = Click2Cwl.convert_type(param)
            cwl_param.update(cwl_type)
            cwl['inputs'][param.name] = cwl_param

        return cwl

    @classmethod
    def fill_outputs(cls, cwl: dict, forwards: Sequence[str]) -> dict:
        """ Fills a cwl with the input informations

        :param cwl: the cwl file
        :type cwl: dict
        :param forwards: the inputs to forward to output
        :type forwards: tuple
        :return: the filled cwl file
        :rtype: dict
        """

        for fw in forwards:
            output_name = f'_{fw}'
            cwl['outputs'][output_name] = dict()
            output_type = cwl['inputs'][fw]['type']
            output_type = output_type[-1]
            if isinstance(output_type, str):
                cwl['outputs'][output_name]['type'] = output_type
            elif isinstance(output_type, dict):
                cwl['outputs'][output_name]['type'] = dict()
                cwl['outputs'][output_name]['type']['type'] = output_type['type']
                cwl['outputs'][output_name]['type']['items'] = output_type['items']
            cwl['outputs'][output_name]['outputBinding'] = dict()
            cwl['outputs'][output_name]['outputBinding']['outputEval'] = f'$(inputs.{fw})'

        return cwl

    @classmethod
    def convert_type(cls, param: click.Option) -> dict:
        """ Converts a click type into a cwl type

        :param param: the click option
        :type param: click.Option
        :return: the cwl type
        :rtype: dict
        """

        cwl_types = dict()
        cwl_types['type'] = []
        if not param.required:
            cwl_types['type'].append('null')

        if not param.multiple and param.nargs == 1:
            if isinstance(param.type, click.types.StringParamType) or isinstance(param.type, click.Choice):
                cwl_type = 'string'
            elif isinstance(param.type, click.types.IntParamType):
                cwl_type = 'int'
            elif isinstance(param.type, click.types.FloatParamType):
                cwl_type = 'float'
            elif isinstance(param.type, click.types.BoolParamType):
                cwl_type = 'boolean'
            # elif isinstance(param.type, click.Choice):
            #     cwl_type = dict()
            #     cwl_type['type'] = 'enum'
            #     cwl_type['symbols'] = param.type.choices
            elif isinstance(param.type, click.Tuple):
                cwl_type = 'string'

        elif (not param.multiple and param.nargs > 1) or (param.multiple and param.nargs == 1):
            cwl_type = dict()
            cwl_type['type'] = 'array'

            if isinstance(param.type, click.types.StringParamType) or isinstance(param.type, click.Choice):
                cwl_type['items'] = 'string'
            elif isinstance(param.type, click.types.IntParamType):
                cwl_type['items'] = 'int'
            elif isinstance(param.type, click.types.FloatParamType):
                cwl_type['items'] = 'float'
            elif isinstance(param.type, click.types.BoolParamType):
                cwl_type['items'] = 'boolean'
            # elif isinstance(param.type, click.Choice):
            #     cwl_type['items'] = dict()
            #     cwl_type['items']['type'] = 'enum'
            #     cwl_type['items']['symbols'] = param.type.choices
            elif isinstance(param.type, click.Tuple):
                cwl_type['items'] = 'string'

        elif param.multiple and param.nargs > 1:

            cwl_type = dict()
            cwl_type['type'] = 'array'

            if isinstance(param.type, click.types.StringParamType) or isinstance(param.type, click.Choice):
                cwl_type['items'] = dict()
                cwl_type['items']['type'] = 'array'
                cwl_type['items']['items'] = 'string'
            elif isinstance(param.type, click.types.IntParamType):
                cwl_type['items'] = dict()
                cwl_type['items']['type'] = 'array'
                cwl_type['items']['items'] = 'int'
            elif isinstance(param.type, click.types.FloatParamType):
                cwl_type['items'] = dict()
                cwl_type['items']['type'] = 'array'
                cwl_type['items']['items'] = 'float'

            # can't have bool with nargs > 1

            # elif isinstance(param.type, click.Choice):
            #     cwl_type['items'] = dict()
            #     cwl_type['items']['type'] = 'array'
            #     cwl_type['items']['items'] = dict()
            #     cwl_type['items']['items']['type'] = 'enum'
            #     cwl_type['items']['items']['symbols'] = param.type.choices
            elif isinstance(param.type, click.Tuple):
                cwl_type['items'] = dict()
                cwl_type['items']['type'] = 'array'
                cwl_type['items']['items'] = 'string'

        cwl_types['type'].append(cwl_type)

        if not param.multiple:
            if not param.required and param.default is not None:
                cwl_types['default'] = param.default
            cwl_types['inputBinding'] = dict()
            cwl_types['inputBinding']['prefix'] = param.opts[0]
        elif param.multiple:
            if not param.required and param.default is not None:
                cwl_types['type'][-1]['default'] = param.default
            cwl_types['type'][-1]['inputBinding'] = dict()
            cwl_types['type'][-1]['inputBinding']['prefix'] = param.opts[0]

        return cwl_types

    @classmethod
    def convert_click_to_cwl(cls, cmd: click.Command, cli_path: list, forwards: Sequence[str]) -> dict:
        cwl_script = cls.cwl_template()
        cwl_script = cls.fill_command(cwl_script, cmd, cli_path)
        cwl_script = cls.fill_inputs(cwl_script, cmd)
        if len(forwards) > 0:
            cwl_script = cls.fill_outputs(cwl_script, forwards)
        return cwl_script

    @classmethod
    def save_cwl(cls, cwl: dict, output_file: str):
        """ Saves a cwl to file

        :param cwl: the cwl file
        :type cwl: dict
        :param output_file: the output file
        :type output_file: str
        """

        class NoAliasDumper(yaml.SafeDumper):
            def ignore_aliases(self, data):
                return True

        with open(output_file, 'w') as f:
            yaml.dump(cwl, f, Dumper=NoAliasDumper, sort_keys=False)
        # reads and writes again to add the first line
        with open(output_file, 'r') as f:
            lines = f.readlines()
        lines.insert(0, '#!/usr/bin/env cwl-runner\n')
        with open(output_file, 'w') as f:
            f.writelines(lines)