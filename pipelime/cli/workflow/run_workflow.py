import click


@click.command("run", help="Runs a cwl workflow")
@click.option("--cwl_file", required=True, type=str, help="The cwl workflow file")
@click.option("--yml_file", required=True, type=str, help="The XConfig input file, placeholders will be prompted to the user")
@click.option("--parallel", required=False, is_flag=True, help="Enable parallel execution")
@click.option("--debug", required=False, is_flag=True, help="Print even more logging")
def run(
    cwl_file,
    yml_file,
    parallel,
    debug
):
    import tempfile
    import subprocess
    from choixe.configurations import XConfig
    from choixe.inquirer import XInquirer

    # reads and prompts the XConfig
    cfg = XConfig(filename=yml_file)
    cfg = XInquirer.prompt(cfg)
    compiled_cfg_file = f'{tempfile.NamedTemporaryFile().name}.yml'
    cfg.save_to(compiled_cfg_file)

    # prepares the cwl-runner
    cmd = ['cwl-runner']
    if parallel:
        cmd.append('--parallel')
    if debug:
        cmd.append('--debug')
    cmd.append(cwl_file)
    cmd.append(compiled_cfg_file)

    # runs workflow
    subprocess.run(cmd)


if __name__ == "__main__":
    run()
