import click


@click.command("show_graph", help="Show cwl workflow graph with using dot notation")
@click.option("--cwl_file", required=True, type=str, help="The cwl workflow file")
def show_graph(cwl_file):
    import os
    import subprocess
    import tempfile

    from networkx.drawing.nx_agraph import read_dot, to_agraph

    def show_nx_graph(graph):
        agraph = to_agraph(graph)
        agraph.layout("dot")
        agraph_file = f"{tempfile.NamedTemporaryFile().name}.svg"
        agraph.draw(agraph_file)
        os.system(f"eog {agraph_file}")

    dot_file = f"{tempfile.NamedTemporaryFile().name}.txt"
    with open(dot_file, "w") as f:
        subprocess.run(["cwl-runner", "--print-dot", cwl_file], stdout=f)
    graph = read_dot(dot_file)
    show_nx_graph(graph)


if __name__ == "__main__":
    show_graph()
