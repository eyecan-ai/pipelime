import click


@click.command("update_nodes", help="Updates nodes")
@click.option(
    "--folder",
    default=None,
    type=str,
    help="Folder of the cwl. Leave empty to use default folder",
)
def update_nodes(folder):

    import inquirer
    import rich

    from pipelime.workflow.cwl import CwlNodesManager, CwlTemplate

    # retrieves available nodes
    nodes = CwlNodesManager.available_nodes(folder=folder)

    # prompt
    answers = inquirer.prompt(
        [
            inquirer.Checkbox(
                "nodes",
                message="Which nodes do you want to update?",
                choices=nodes.keys(),
            ),
        ]
    )

    # update nodes
    for name, node in nodes.items():
        if name in answers["nodes"]:
            rich.print(f'[green] Update node "{name}"')
            available_names = [x for x in nodes.keys() if x != name]
            update_answers = inquirer.prompt(
                [
                    inquirer.Text(
                        "name",
                        message="Update name",
                        default=node.name,
                        validate=lambda _, x: x not in available_names,
                    ),
                    inquirer.Text(
                        "script",
                        message="Update script",
                        default=node.cwl_template.script,
                    ),
                    inquirer.Text(
                        "alias",
                        message="Update alias",
                        default=" ".join(node.cwl_template.alias),
                    ),
                    inquirer.Checkbox(
                        "forwards",
                        message="Update forwards",
                        choices=node.cwl_template.inputs_keys,
                        default=node.cwl_template.forwards,
                    ),
                ]
            )

            CwlNodesManager.delete_node(name, folder=folder)
            cwl_template = CwlTemplate(
                script=update_answers["script"],
                alias=update_answers["alias"].split(" "),
                forwards=update_answers["forwards"],
            )
            CwlNodesManager.create_node(
                update_answers["name"], cwl_template=cwl_template, folder=folder
            )


if __name__ == "__main__":
    update_nodes()
