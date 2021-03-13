from pathlib import Path
import click
import yaml
from pipelime.workflow.cwl import CwlTemplate


class TestClick2Cwl(object):

    def test_cwl_template(self, tmpdir_factory):

        # int
        opt01 = click.Option(param_decls=['--opt01'], required=True, type=int, help='help opt01')
        opt01not = click.Option(param_decls=['--opt01not'], required=False, default=10, type=int, help='help opt01not')
        opt02 = click.Option(param_decls=['--opt02'], required=True, nargs=2, type=int, help='help opt02')
        opt03 = click.Option(param_decls=['--opt03'], required=True, multiple=True, type=int, help='help opt03')
        opt04 = click.Option(param_decls=['--opt04'], required=True, nargs=2, multiple=True, type=int, help='help opt04')
        # float
        opt11 = click.Option(param_decls=['--opt11'], required=True, type=float, help='help opt11')
        opt12 = click.Option(param_decls=['--opt12'], required=True, nargs=2, type=float, help='help opt12')
        opt13 = click.Option(param_decls=['--opt13'], required=True, multiple=True, type=float, help='help opt13')
        opt14 = click.Option(param_decls=['--opt14'], required=True, nargs=2, multiple=True, type=float, help='help opt14')
        # str
        opt21 = click.Option(param_decls=['--opt21'], required=True, type=str, help='help opt21')
        opt22 = click.Option(param_decls=['--opt22'], required=True, nargs=2, type=str, help='help opt22')
        opt23 = click.Option(param_decls=['--opt23'], required=True, multiple=True, type=str, help='help opt23')
        opt24 = click.Option(param_decls=['--opt24'], required=True, nargs=2, multiple=True, type=str, help='help opt24')
        # bool
        opt31 = click.Option(param_decls=['--opt31'], required=True, is_flag=True, help='help opt31')
        opt33 = click.Option(param_decls=['--opt33'], required=True, multiple=True, is_flag=True, help='help opt33')
        # choice
        opt41 = click.Option(param_decls=['--opt41'], required=True, type=click.Choice(['a', 'b']), help='help opt41')
        opt42 = click.Option(param_decls=['--opt42'], required=True, nargs=2, type=click.Choice(['a', 'b']), help='help opt42')
        opt43 = click.Option(param_decls=['--opt43'], required=True, multiple=True, type=click.Choice(['a', 'b']), help='help opt43')
        opt44 = click.Option(param_decls=['--opt44'], required=True, nargs=2, multiple=True, type=click.Choice(['a', 'b']), help='help opt44')
        # tuple
        opt51 = click.Option(param_decls=['--opt51'], required=True, type=(str, str), help='help opt51')
        opt52 = click.Option(param_decls=['--opt52'], required=True, nargs=2, type=(str, str), help='help opt52')
        opt53 = click.Option(param_decls=['--opt53'], required=True, multiple=True, type=(str, str), help='help opt53')
        opt54 = click.Option(param_decls=['--opt54'], required=True, nargs=2, multiple=True, type=(str, str), help='help opt54')

        params = [opt01, opt01not, opt02, opt03, opt04,
                  opt11, opt12, opt13, opt14,
                  opt21, opt22, opt23, opt24,
                  opt31, opt33,
                  opt41, opt42, opt43, opt44,
                  opt51, opt52, opt53, opt54]

        click_help = 'this is the click help'
        cmd = click.Command('this is the click name', help=click_help, params=params)
        alias = ['first', 'second', 'third']
        forwards = ['opt01', 'opt02', 'opt03', 'opt04']
        cwl_template = CwlTemplate.from_command(cmd, alias=alias, forwards=forwards)

        cwl_keys = ['cwlVersion', 'class', 'doc', 'baseCommand', 'inputs', 'outputs']
        for key in cwl_keys:
            assert key in cwl_template.template.keys()
        assert cwl_template.template['doc'] == click_help
        assert cwl_template.template['baseCommand'][0] == 'first'
        assert cwl_template.template['baseCommand'][1] == 'second'
        assert cwl_template.template['baseCommand'][2] == 'third'

        # int
        expected_cwl = {
            'doc': 'help opt01',
            'type': [
                'int'
            ],
            'inputBinding': {'prefix': '--opt01'}
        }
        assert 'opt01' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt01'] == expected_cwl

        expected_cwl = {
            'doc': 'help opt01not',
            'type': [
                'null',
                'int'
            ],
            'default': 10,
            'inputBinding': {'prefix': '--opt01not'}
        }
        assert 'opt01not' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt01not'] == expected_cwl

        expected_cwl = {
            'doc': 'help opt02',
            'type': [{
                'type': 'array',
                'items': 'int'
            }],
            'inputBinding': {'prefix': '--opt02'}
        }
        assert 'opt02' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt02'] == expected_cwl

        expected_cwl = {
            'doc': 'help opt03',
            'type': [{
                'type': 'array',
                'items': 'int',
                'inputBinding': {'prefix': '--opt03'}
            }]
        }
        assert 'opt03' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt03'] == expected_cwl

        expected_cwl = {
            'doc': 'help opt04',
            'type': [{
                'type': 'array',
                'items': {
                        'type': 'array',
                        'items': 'int'
                },
                'inputBinding': {'prefix': '--opt04'}
            }]
        }
        assert 'opt04' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt04'] == expected_cwl

        # float
        expected_cwl = {
            'doc': 'help opt11',
            'type': [
                'float'
            ],
            'inputBinding': {'prefix': '--opt11'}
        }
        assert 'opt11' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt11'] == expected_cwl

        expected_cwl = {
            'doc': 'help opt12',
            'type': [{
                'type': 'array',
                'items': 'float'
            }],
            'inputBinding': {'prefix': '--opt12'}
        }
        assert 'opt12' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt12'] == expected_cwl

        expected_cwl = {
            'doc': 'help opt13',
            'type': [{
                'type': 'array',
                'items': 'float',
                'inputBinding': {'prefix': '--opt13'}
            }]
        }
        assert 'opt13' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt13'] == expected_cwl

        expected_cwl = {
            'doc': 'help opt14',
            'type': [{
                'type': 'array',
                'items': {
                        'type': 'array',
                        'items': 'float'
                },
                'inputBinding': {'prefix': '--opt14'}
            }]
        }
        assert 'opt14' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt14'] == expected_cwl

        # string
        expected_cwl = {
            'doc': 'help opt21',
            'type': [
                'string'
            ],
            'inputBinding': {'prefix': '--opt21'}
        }
        assert 'opt21' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt21'] == expected_cwl

        expected_cwl = {
            'doc': 'help opt22',
            'type': [{
                'type': 'array',
                'items': 'string'
            }],
            'inputBinding': {'prefix': '--opt22'}
        }
        assert 'opt22' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt22'] == expected_cwl

        expected_cwl = {
            'doc': 'help opt23',
            'type': [{
                'type': 'array',
                'items': 'string',
                'inputBinding': {'prefix': '--opt23'}
            }]
        }
        assert 'opt23' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt23'] == expected_cwl

        expected_cwl = {
            'doc': 'help opt24',
            'type': [{
                'type': 'array',
                'items': {
                        'type': 'array',
                        'items': 'string'
                },
                'inputBinding': {'prefix': '--opt24'}
            }]
        }
        assert 'opt24' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt24'] == expected_cwl

        # bool
        expected_cwl = {
            'doc': 'help opt31',
            'type': [
                'boolean'
            ],
            'inputBinding': {'prefix': '--opt31'}
        }
        assert 'opt31' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt31'] == expected_cwl

        expected_cwl = {
            'doc': 'help opt33',
            'type': [{
                'type': 'array',
                'items': 'boolean',
                'inputBinding': {'prefix': '--opt33'}
            }]
        }
        assert 'opt33' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt33'] == expected_cwl

        # choice
        expected_cwl = {
            'doc': 'help opt41',
            'type': [
                'string'
            ],
            'inputBinding': {'prefix': '--opt41'}
        }
        assert 'opt41' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt41'] == expected_cwl

        expected_cwl = {
            'doc': 'help opt42',
            'type': [{
                'type': 'array',
                'items': 'string'
            }],
            'inputBinding': {'prefix': '--opt42'}
        }
        assert 'opt42' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt42'] == expected_cwl

        expected_cwl = {
            'doc': 'help opt43',
            'type': [{
                'type': 'array',
                'items': 'string',
                'inputBinding': {'prefix': '--opt43'}
            }]
        }
        assert 'opt43' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt43'] == expected_cwl

        expected_cwl = {
            'doc': 'help opt44',
            'type': [{
                'type': 'array',
                'items': {
                        'type': 'array',
                        'items': 'string'
                },
                'inputBinding': {'prefix': '--opt44'}
            }]
        }
        assert 'opt44' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt44'] == expected_cwl

        # tuple
        expected_cwl = {
            'doc': 'help opt51',
            'type': [{
                'type': 'array',
                'items': 'string'
            }],
            'inputBinding': {'prefix': '--opt51'}
        }
        assert 'opt51' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt51'] == expected_cwl

        expected_cwl = {
            'doc': 'help opt52',
            'type': [{
                'type': 'array',
                'items': 'string'
            }],
            'inputBinding': {'prefix': '--opt52'}
        }
        assert 'opt52' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt52'] == expected_cwl

        expected_cwl = {
            'doc': 'help opt53',
            'type': [{
                'type': 'array',
                'items': {
                        'type': 'array',
                        'items': 'string'
                },
                'inputBinding': {'prefix': '--opt53'}
            }]
        }
        assert 'opt53' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt53'] == expected_cwl

        expected_cwl = {
            'doc': 'help opt54',
            'type': [{
                'type': 'array',
                'items': {
                        'type': 'array',
                        'items': 'string'
                },
                'inputBinding': {'prefix': '--opt54'}
            }]
        }
        assert 'opt54' in cwl_template.template['inputs'].keys()
        assert cwl_template.template['inputs']['opt54'] == expected_cwl

        assert '_opt01' in cwl_template.template['outputs'].keys()
        assert cwl_template.template['outputs']['_opt01']['type'] == 'int'
        assert cwl_template.template['outputs']['_opt01']['outputBinding']['outputEval'] == '$(inputs.opt01)'

        assert '_opt02' in cwl_template.template['outputs'].keys()
        assert cwl_template.template['outputs']['_opt02']['type']['type'] == 'array'
        assert cwl_template.template['outputs']['_opt02']['type']['items'] == 'int'
        assert cwl_template.template['outputs']['_opt02']['outputBinding']['outputEval'] == '$(inputs.opt02)'

        assert '_opt03' in cwl_template.template['outputs'].keys()
        assert cwl_template.template['outputs']['_opt03']['type']['type'] == 'array'
        assert cwl_template.template['outputs']['_opt03']['type']['items'] == 'int'
        assert cwl_template.template['outputs']['_opt03']['outputBinding']['outputEval'] == '$(inputs.opt03)'

        assert '_opt04' in cwl_template.template['outputs'].keys()
        assert cwl_template.template['outputs']['_opt04']['type']['type'] == 'array'
        assert cwl_template.template['outputs']['_opt04']['type']['items']['type'] == 'array'
        assert cwl_template.template['outputs']['_opt04']['type']['items']['items'] == 'int'
        assert cwl_template.template['outputs']['_opt04']['outputBinding']['outputEval'] == '$(inputs.opt04)'

        # cwl_filled2 = Click2Cwl.convert_click_to_cwl(cmd, commands, forwards)
        # assert cwl_filled == cwl_filled2

        output_cwl_file = Path(tmpdir_factory.mktemp('cwl')) / f'output.cwl'
        cwl_template.save_to(output_cwl_file)
        with open(output_cwl_file, 'r') as f:
            lines = f.readlines()
            assert lines[0] == '#!/usr/bin/env cwl-runner\n'
            loaded_cwl = yaml.safe_load(''.join(lines))
            assert cwl_template.template == loaded_cwl

        click_command = f"""
import click
@click.command('this is the click name', help='{click_help}')
@click.option('--opt0', required=True, type=int, help='help opt0')
@click.option('--opt1', required=True, type=float, help='help opt1')
@click.option('--opt2', required=False, type=str, default='default2', help='help opt2')
@click.option('--opt3', required=False, is_flag=True, help='help opt3')
@click.option('--opt4', required=False, type=(int, str), help='help opt4')
@click.option('--opt5', required=False, default='c1', type=click.Choice(['c1', 'c2']), help='help opt5')
@click.option('--opt6', required=False, type=int, nargs=3, help='help opt6')
@click.option('--opt7', required=False, multiple=True, type=int, help='help opt7')
@click.option('--opt8', required=False, multiple=True, type=(int, str), help='help opt7')
def a_click_command(opt0, opt1, opt2, opt3, opt4, opt5, opt6, opt7, opt8):
    pass
        """

        output_click_file = Path(tmpdir_factory.mktemp('click')) / f'output.py'
        with open(output_click_file, 'w') as f:
            f.write(click_command)
        loaded_template = CwlTemplate(output_click_file)

        cwl_keys = ['cwlVersion', 'class', 'doc', 'baseCommand', 'inputs', 'outputs']
        for key in cwl_keys:
            assert key in loaded_template.template.keys()
        assert loaded_template.template['doc'] == click_help
        assert len(loaded_template.template['inputs'].keys()) == 9
