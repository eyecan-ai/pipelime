import click


@click.command("delete_nodes", help="Delete nodes")
@click.option(
    "--folder",
    default=None,
    type=str,
    help="Folder of the cwl. Leave empty to use default folder",
)
def delete_nodes(folder):

    import inquirer

    from pipelime.workflow.cwl import CwlNodesManager

    # retrieves available nodes
    nodes = CwlNodesManager.available_nodes(folder=folder)

    # prompt
    answers = inquirer.prompt(
        [
            inquirer.Checkbox(
                "nodes",
                message="Which nodes do you want to delete?",
                choices=nodes.keys(),
            ),
        ]
    )

    # delete nodes
    for node in answers["nodes"]:
        CwlNodesManager.delete_node(node, folder)


if __name__ == "__main__":
    delete_nodes()
