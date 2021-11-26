import click


@click.command("run", help="Runs a cwl workflow")
@click.option("--workflow", required=True, type=str, help="The cwl workflow file")
@click.option(
    "--cfg_file",
    required=True,
    type=str,
    help="The configuration input file, XConfig placeholders will be prompted to the user",
)
@click.option(
    "--folder",
    default=None,
    type=str,
    help="Folder of the cwl nodes. Leave empty to use default folder",
)
@click.option(
    "--fill/--no-fill",
    default=True,
    type=bool,
    help="Do not fill the workflow steps with the cwl nodes path",
)
@click.option(
    "--parallel", required=False, is_flag=True, help="Enable parallel execution"
)
@click.option("--debug", required=False, is_flag=True, help="Print even more logging")
def run(workflow, cfg_file, folder, fill, parallel, debug):
    import subprocess
    import tempfile

    from choixe.configurations import XConfig
    from choixe.inquirer import XInquirer

    from pipelime.workflow.cwl import CwlNodesManager, CwlWorkflowTemplate

    # reads and prompts the XConfig
    cfg = XConfig(filename=cfg_file)
    cfg = XInquirer.prompt(cfg)
    compiled_cfg_file = f"{tempfile.NamedTemporaryFile().name}.yml"
    cfg.save_to(compiled_cfg_file)

    if fill:
        workflow_template = CwlWorkflowTemplate.from_file(workflow)
        workflow_template = CwlNodesManager.fill_workflow(
            workflow_template, folder=folder
        )
        workflow_file = f"{tempfile.NamedTemporaryFile().name}.cwl"
        workflow_template.dumps(workflow_file)
        workflow = workflow_file

    # prepares the cwl-runner
    cmd = ["cwl-runner"]
    if parallel:
        cmd.append("--parallel")
    if debug:
        cmd.append("--debug")
    cmd.append(workflow)
    cmd.append(compiled_cfg_file)

    # runs workflow
    subprocess.run(cmd)


if __name__ == "__main__":
    run()
