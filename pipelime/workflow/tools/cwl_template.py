import importlib.util
from typing import Union, Sequence
from pathlib import Path
import click
from click.decorators import command
import yaml


class CwlTemplate(object):
    
    def __init__(self, script: str = None, alias: list = None, forwards: list = None):
        """ Creates a cwl template from script containing click.Command

        :param script: the script, defaults to None
        :type script: str, optional
        :param alias: the commands needed to call the script, defaults to None
        :type alias: list, optional
        :param forwards: the input parameters to forward to output, defaults to None
        :type forwards: list, optional
        """

        self._script = script
        self._cmd = None
        if self._script is not None:
            self._cmd = self._load_click_command(self._script)
        self._alias = alias
        self._forwards = forwards
        self._template = dict()
        if self._cmd is not None:
            self._fill()

    @property
    def template(self):
        return self._template

    @classmethod
    def from_command(cls, command: click.Command, alias: list = None, forwards: list = None) -> 'CwlTemplate':
        """ Creates a cwl template directly from click.Command

        :param command: the click command
        :type command: click.Command
        :param alias: the commands needed to call the script, defaults to None
        :type alias: list, optional
        :param forwards: the input parameters to forward to output, defaults to None
        :type forwards: list, optional
        :return: a cwl template
        :rtype: CwlTemplate
        """

        cwl_template = CwlTemplate(alias=alias, forwards=forwards)
        cwl_template._cmd = command
        cwl_template._fill()
        return cwl_template

    def _fill(self):
        """ Fills the template
        """
        self._init_template()
        if self._alias is not None:
            self._fill_command(self._alias)
        self._fill_inputs(self._cmd)
        if self._forwards is not None:
            self._fill_outputs(self._forwards)


    def _load_click_command(self, script: str) -> Union[click.Command, None]:
        """ Loads a click.Command from file, it assumes
        that the script contains only one command,
        if there are no commands it will return None

        :param script: the script filename
        :type script: str
        :return: the click command
        :rtype: Union[click.Command, None]
        """

        spec = importlib.util.spec_from_file_location(script.stem, script)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        cmd = None
        for x in dir(module):
            x = getattr(module, x)
            if isinstance(x, click.core.Command):
                cmd = x
                break

        assert cmd is not None, "the script doesn't contain any click.Command"

        return cmd

    def _init_template(self):
        """ Initialize the cwl template for a CommandLineTool
        """

        self._template['cwlVersion'] = 'v1.0'
        self._template['class'] = 'CommandLineTool'
        self._template['doc'] = ''
        self._template['baseCommand'] = ''
        self._template['inputs'] = dict()
        self._template['outputs'] = dict()
    
    def _fill_command(self, commands: list):
        """ Fills the cwl template with the commands informations

        :param commands: the commands needed to call the script
        :type commands: list
        """
        
        self._template['baseCommand'] = commands

    def _fill_inputs(self, cmd: click.Command):
        """ Fills the cwl template with input informations

        :param cmd: [description]
        :type cmd: click.Command
        """

        self._template['doc'] = cmd.help
        for param in cmd.params:
            param: click.Option
            cwl_param = dict()
            cwl_param['doc'] = param.help
            cwl_type = self._convert_type(param)
            cwl_param.update(cwl_type)
            self._template['inputs'][param.name] = cwl_param

    def _convert_type(self, param: click.Option) -> dict:
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
            elif isinstance(param.type, click.Tuple):
                cwl_type['items'] = 'string'

        # can't have bool with nargs > 1
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

    def _fill_outputs(self, forwards: list) :
        """ Fills the cwl template with outputs informations

        :param forwards: the input parameters to forward to output
        :type forwards: list
        """

        for fw in forwards:
            output_name = f'_{fw}'
            self._template['outputs'][output_name] = dict()
            output_type = self._template['inputs'][fw]['type']
            output_type = output_type[-1]
            if isinstance(output_type, str):
                self._template['outputs'][output_name]['type'] = output_type
            elif isinstance(output_type, dict):
                self._template['outputs'][output_name]['type'] = dict()
                self._template['outputs'][output_name]['type']['type'] = output_type['type']
                self._template['outputs'][output_name]['type']['items'] = output_type['items']
            self._template['outputs'][output_name]['outputBinding'] = dict()
            self._template['outputs'][output_name]['outputBinding']['outputEval'] = f'$(inputs.{fw})'

    def save_to(self, filename: str):
        """ Saves the cwl to filename

        :param filename: the output filename
        :type filename: str
        """

        # need to avoid that pyyaml inserts aliases
        class NoAliasDumper(yaml.SafeDumper):
            def ignore_aliases(self, data):
                return True

        with open(filename, 'w') as f:
            yaml.dump(self._template, f, Dumper=NoAliasDumper, sort_keys=False)
        # reads and writes again to add the first line
        with open(filename, 'r') as f:
            lines = f.readlines()
        lines.insert(0, '#!/usr/bin/env cwl-runner\n')
        with open(filename, 'w') as f:
            f.writelines(lines)
