import click


@click.command("fill", help="Fill a workflow with the cwl paths")
@click.option("--workflow", required=True, type=str, help="Output filename of the cwl workflow file")
@click.option("--output_filename", required=True, type=str, help="Output filename of the cwl workflow file")
@click.option("--folder", default=None, type=str, help="Folder of the cwl. Leave empty to use default folder")
def fill(workflow, output_filename, folder):

    from pipelime.workflow.cwl import CwlNodesManager
    from pipelime.workflow.cwl import CwlWorkflowTemplate

    workflow_template = CwlWorkflowTemplate.from_file(workflow)
    workflow_template = CwlNodesManager.fill_workflow(workflow_template, folder=folder)
    workflow_template.dumps(output_filename)


if __name__ == "__main__":
    fill()
