import click


@click.command("list_nodes", help="List available nodes")
@click.option(
    "--folder",
    default=None,
    type=str,
    help="Folder of the cwl. Leave empty to use default folder",
)
def list_nodes(folder):

    import rich
    from rich.tree import Tree

    from pipelime.workflow.cwl import CwlNodesManager

    # retrieves available nodes
    nodes = CwlNodesManager.available_nodes(folder=folder)

    # draw tree
    t_root = Tree("🧩 [green]Nodes")
    for name, node in nodes.items():
        if not node.is_valid:
            t_node = Tree(
                f"[red][u]{name} (node NOT valid, can't load the script)[/u][/red]"
            )
        else:
            t_node = Tree(f"[u]{name}[/u]")
        t_node.add(f"[blue]Path:[/blue] {node.cwl_path}")
        t_template = Tree("[blue]Template[/blue]")
        t_template.add(f"Script: {node.cwl_template.script}")
        t_template.add(f"Alias: {node.cwl_template.alias}")
        t_template.add(f"Forwards: {node.cwl_template.forwards}")
        t_node.add(t_template)
        t_root.add(t_node)
    rich.print(t_root)


if __name__ == "__main__":
    list_nodes()
