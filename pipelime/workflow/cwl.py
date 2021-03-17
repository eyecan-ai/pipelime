from pipelime.cli.workflow.workflow import workflow
import typing
import os
from choixe.spooks import Spook
import importlib.util
from typing import Dict, Union, Sequence
from pathlib import Path
import click
from schema import Or
import yaml
from pipelime import user_data_dir
from choixe.configurations import XConfig


class CwlNodesManager(object):

    DEFAULT_CWL_EXTENSION = 'cwl'
    DEFAULT_META_EXTENSION = 'yml'

    @classmethod
    def nodes_folder(cls) -> Path:
        """ Default nodes folder

        :return: folder
        :rtype: Path
        """

        folder = user_data_dir() / cls.__name__
        if not folder.exists():
            folder.mkdir(parents=True, exist_ok=True)
        return folder

    @classmethod
    def get_associated_meta_path(cls, p: Path) -> Path:
        """ Gets cwl file associated metadata path

        :param p: cwl file
        :type p: Path
        :return: metadata file
        :rtype: Path
        """

        return p.parent / f'{p.stem}.{cls.DEFAULT_META_EXTENSION}'

    @classmethod
    def is_valid_cwl_file(cls, p: Path) -> bool:
        """ Checks for valid cwl file

        :param p: cwl file
        :type p: Path
        :return: TRUE if is valid cwl file
        :rtype: bool
        """

        return p.is_file() and cls.get_associated_meta_path(p).exists()

    @classmethod
    def load_cwl_node(cls, p: Path) -> 'CwlNode':
        """ Loads cwl node from path

        :return: loaded CwlNode
        :rtype: CwlNode
        """

        if cls.is_valid_cwl_file(p):
            meta_p = cls.get_associated_meta_path(p)
            cwl_template = Spook.create(d=XConfig(filename=meta_p).to_dict())
            cwl_node = CwlNode(name=p.stem, cwl_path=str(p), cwl_template=cwl_template)
            return cwl_node
        return None

    @classmethod
    def available_nodes(cls, folder: Union[str, None] = None) -> Dict[str, 'CwlNode']:
        """ Gets a map of stored CwlNodes

        :return: stored nodes map [node_name: node]
        :rtype: Dict[str, 'CwlNode']
        """

        folder = Path(folder) if folder is not None else cls.nodes_folder()
        cwls = folder.glob(f'*.{cls.DEFAULT_CWL_EXTENSION}')
        cwls = [x for x in cwls if cls.is_valid_cwl_file(x)]
        nodes = [cls.load_cwl_node(x) for x in cwls]
        nodes = {x.name: x for x in nodes}
        return nodes

    @classmethod
    def create_node(cls, name: str, cwl_template: 'CwlTemplate', folder: Union[str, None] = None) -> 'CwlNode':
        """ Creates a node with input name and CwlTemplate

        :param name: input node name
        :type name: str
        :param cwl_template: input cwl template
        :type cwl_template: CwlTemplate
        :param folder: if None the default folder will be used, defaults to None
        :type folder: Union[str, None], optional
        :raises RuntimeError: if a node with same name found
        :return: created node
        :rtype: CwlNode
        """

        nodes = cls.available_nodes(folder=folder)
        if name in nodes:
            raise RuntimeError(f'Node with same name "{name}" found')

        folder = Path(folder) if folder is not None else cls.nodes_folder()
        cwl_filename = folder / f'{name}.{cls.DEFAULT_CWL_EXTENSION}'
        meta_filename = folder / f'{name}.{cls.DEFAULT_META_EXTENSION}'

        cwl_template.dumps(cwl_filename)
        XConfig.from_dict(d=cwl_template.serialize()).save_to(meta_filename)

        return CwlNode(
            name=name,
            cwl_path=str(cwl_filename),
            cwl_template=cwl_template
        )

    @classmethod
    def delete_node(cls, name: str, folder: Union[str, None] = None):
        """ Deletes a node given the name

        :param name: node to delete name
        :type name: str
        :param folder: if None the default folder will be used, defaults to None
        :type folder: Union[str, None], optional
        :raises RuntimeError: if a node with this name not found
        """

        nodes = cls.available_nodes(folder=folder)
        if name not in nodes:
            raise RuntimeError(f'Node with name "{name}" not found')

        folder = Path(folder) if folder is not None else cls.nodes_folder()
        cwl_filename = folder / f'{name}.{cls.DEFAULT_CWL_EXTENSION}'
        meta_filename = folder / f'{name}.{cls.DEFAULT_META_EXTENSION}'

        os.remove(cwl_filename)
        os.remove(meta_filename)

    @classmethod
    def initialize_workflow(cls, names: Sequence[str], folder: Union[str, None] = None) -> 'CwlWorkflowTemplate':

        nodes = cls.available_nodes(folder)
        workflow_nodes = []
        for name in names:
            if name not in nodes:
                raise RuntimeError(f'Node with name "{name}" not found')
            workflow_nodes.append([v for k, v in nodes.items() if k == name][0])

        workflow_template = CwlWorkflowTemplate(workflow_nodes)

        return workflow_template


class CwlTemplate(Spook):

    def __init__(self, script: str = None, alias: Sequence[str] = None, forwards: Sequence[str] = None):
        """ Creates a cwl template from script containing click.Command

        :param script: the script, defaults to None
        :type script: str, optional
        :param alias: the commands needed to call the script, defaults to None
        :type alias: Sequence[str], optional
        :param forwards: the input parameters to forward to output, defaults to None
        :type forwards: Sequence[str], optional
        """

        super().__init__()

        self._script = script
        self._alias = alias
        self._forwards = forwards

        self._cmd = None
        if self._script is not None:
            self._cmd = self._load_click_command(self.resolved_script)
        self._template = dict()
        if self._cmd is not None:
            self._fill()

    @ classmethod
    def spook_schema(cls) -> typing.Union[None, dict]:
        return {
            'script': Or(str, None),
            'alias': Or([str], None),
            'forwards': Or([str], None),
        }

    @ classmethod
    def from_dict(cls, d: dict):
        return cls(**d)

    def to_dict(self) -> dict:
        return {
            'script': self._script,
            'alias': self._alias,
            'forwards': self._forwards
        }

    @property
    def script(self):
        return self._script

    @property
    def resolved_script(self):
        return Path(self._script).absolute().resolve()

    @property
    def command(self):
        return self._cmd

    @property
    def template(self):
        return self._template

    @property
    def alias(self):
        return self._alias

    @property
    def forwards(self):
        return self._forwards

    @property
    def inputs(self):
        return self._template.get('inputs', None)

    @property
    def inputs_keys(self):
        if self.inputs is not None:
            return list(self.inputs.keys())
        return []

    @property
    def outputs(self):
        return self._template.get('outputs', None)

    @property
    def outputs_keys(self):
        if self.outputs is not None:
            return list(self.outputs.keys())
        return []

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
        try:
            spec = importlib.util.spec_from_file_location(script.stem, script)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            cmd = None
            for x in dir(module):
                x = getattr(module, x)
                if isinstance(x, click.core.Command):
                    cmd = x
                    break
        except Exception:
            cmd = None

        return cmd

    def _init_template(self):
        """ Initializes the cwl template for a CommandLineTool
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

        :param cmd: the click command
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

    def _fill_outputs(self, forwards: list):
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

    def dumps(self, path: str):
        """ Dumps the cwl template to target file

        :param path: the output complete filename
        :type path: str
        """

        class NoAliasDumper(yaml.SafeDumper):
            def ignore_aliases(self, data):
                return True

        with open(path, 'w') as f:
            yaml.dump(self._template, f, Dumper=NoAliasDumper, sort_keys=False)
        # reads and writes again to add the first line
        with open(path, 'r') as f:
            lines = f.readlines()
        lines.insert(0, '#!/usr/bin/env cwl-runner\n')
        with open(path, 'w') as f:
            f.writelines(lines)


class CwlNode:

    def __init__(self, name: str, cwl_path: str, cwl_template: CwlTemplate) -> None:
        self._name = name
        self._cwl_path = cwl_path
        self._cwl_template = cwl_template
        self._is_valid = cwl_template.command is not None

    @property
    def name(self):
        return self._name

    @property
    def cwl_path(self):
        return self._cwl_path

    @property
    def cwl_template(self):
        return self._cwl_template

    @property
    def is_valid(self):
        return self._is_valid


class CwlWorkflowTemplate(object):

    def __init__(self, nodes: Sequence[CwlNode]):
        """ Creates a cwl workflow template from list of CwlNode

        :param nodes: steps of the workflow
        :type nodes: Sequence[CwlNode]
        """

        # super().__init__()

        self._nodes = nodes
        self._template = dict()

        self._fill()

    @property
    def template(self):
        return self._template

    def _fill(self):
        """ Fills the template
        """

        self._init_template()
        self._fill_steps(self._nodes)

    def _init_template(self):
        """ Initializes the cwl workflow template for a CommandLineTool
        """

        self._template['cwlVersion'] = 'v1.0'
        self._template['class'] = 'Workflow'
        self._template['requirements'] = {
            'StepInputExpressionRequirement': dict(),
            'InlineJavascriptRequirement': dict(),
            'MultipleInputFeatureRequirement': dict(),
            'ScatterFeatureRequirement': dict()
        }
        self._template['inputs'] = dict()
        self._template['outputs'] = list()
        self._template['steps'] = dict()

    def _fill_steps(self, steps: Sequence[CwlNode]):
        """ Fills the cwl workflow template with steps informations

        :param steps: the steos of the workflow
        :type steps: Sequence[CwlNode]
        """

        # avoids name conflicts when a node is used more than one time
        counter = {x.name: 0 for x in steps}

        for step in steps:
            cwl_step = dict()
            cwl_step['run'] = step.cwl_path
            cwl_step['in'] = {k: '' for k in step.cwl_template.inputs_keys}
            cwl_step['out'] = step.cwl_template.outputs_keys
            step_name = f'{step.name}{counter[step.name]}'
            self._template['steps'][step_name] = cwl_step
            counter[step.name] += 1

            # fill worfkflow inputs based on step inputs
            for input_name, input_opt in step.cwl_template.inputs.items():
                self._template['inputs'][f'{step_name}_{input_name}'] = input_opt['type']

    def dumps(self, path: str):
        """ Dumps the cwl workflow template to target file

        :param path: the output complete filename
        :type path: str
        """

        class NoAliasDumper(yaml.SafeDumper):
            def ignore_aliases(self, data):
                return True

        with open(path, 'w') as f:
            yaml.dump(self._template, f, Dumper=NoAliasDumper, sort_keys=False)
        # reads and writes again to add the first line
        with open(path, 'r') as f:
            lines = f.readlines()
        lines.insert(0, '#!/usr/bin/env cwl-runner\n')
        with open(path, 'w') as f:
            f.writelines(lines)
