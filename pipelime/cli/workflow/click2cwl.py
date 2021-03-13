import click


@click.command("click2cwl", help="Converts a click script to a cwl file")
@click.option("--script", required=True, type=str, help="Script filename")
@click.option("--name", required=True, type=str, help="Cwl template output name")
@click.option("--alias", required=True, type=str, help="Alias command associated to the cwl, e.g.: 'command1 subcommand leaf' with quotes.")
@click.option("--fchoice/--no-fchoice", default=True, type=bool, help="TRUE to activate forwarde interactive choice")
@click.option("--folder", default=None, type=str, help="Output folder to save the cwl. Leave empty to use default folder")
def click2cwl(
    script,
    name,
    alias,
    fchoice,
    folder
):
    from pipelime.workflow.cwl import CwlNodesManager, CwlTemplate

    # Create template
    cwl_template = CwlTemplate(
        script=script,
        alias=alias.split(' '),
        forwards=None
    )

    if fchoice:
        import inquirer
        if len(cwl_template.inputs_keys) > 0:

            # prompt
            answers = inquirer.prompt([
                inquirer.Checkbox(
                    'forwards',
                    message="Which input forward onto output?",
                    choices=cwl_template.inputs_keys,
                ),
            ])

            # Create template
            cwl_template = CwlTemplate(
                script=script,
                alias=alias.split(' '),
                forwards=answers['forwards']
            )

    CwlNodesManager.create_node(
        name=name,
        cwl_template=cwl_template,
        folder=folder if len(folder) > 0 else None
    )


if __name__ == "__main__":
    click2cwl()
