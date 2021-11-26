import click


@click.command("init", help="Initialize a workflow")
@click.option(
    "--output_filename",
    required=True,
    type=str,
    help="Output filename of the cwl workflow file",
)
@click.option(
    "--folder",
    default=None,
    type=str,
    help="Folder of the cwl. Leave empty to use default folder",
)
def init(output_filename, folder):

    import inquirer

    from pipelime.workflow.cwl import CwlNodesManager

    # retrieves available nodes
    nodes = CwlNodesManager.available_nodes(folder=folder)

    choosen_nodes = []
    available_nodes = [k for k, v in nodes.items() if v.is_valid]
    available_nodes.append("END WORKFLOW")
    while True:
        answer = inquirer.prompt(
            [inquirer.List("nodes", message="Choose a node", choices=available_nodes)]
        )
        if answer["nodes"] == "END WORKFLOW":
            break
        choosen_nodes.append(answer["nodes"])

    worlkflow_template = CwlNodesManager.initialize_workflow(choosen_nodes)
    worlkflow_template.dumps(output_filename)


if __name__ == "__main__":
    init()
