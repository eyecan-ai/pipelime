import Operation

G = {
    "params": {
        "input_folders": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"],
        "converted": "/tmp/converted",
        "filtered": "/tmp/filtered",
        "out": "/tmp/output",
        "checked": "/tmp/checked",
        "summed_folder": "/home/summed",
        "splits": [
            {"name": "train", "percentage": 0.8},
            {"name": "validation", "percentage": 0.1},
            {"name": "test", "percentage": 0.1},
        ],
    }
}

DATA = G.params.input_folder

for IDX, D in enumerate(DATA):
    op = Operation("conversion")
    op.IN.input_folder = D
    op.OUT.output_folder = G.params.converted / str(IDX)

op = Operation("underfolder pipelime sum")
op.IN.input_folders = [G.params.converted / str(IDX) for IDX, D in enumerate(DATA)]
op.OUT.output_folder = G.params.summed_folder

op = Operation("filter")
op.IN.input_folder = G.params.summed_folder
op.OUT.output_folder = G.params.filtered

op = Operation("split")
op.IN.input_folder = G.params.filtered
op.OUT.output_folder = [G.params.out / x["name"] for x in G.params.splits]
op.ARGS.splits = [x["percentage"] for x in G.params.splits]

for IDX, S in enumerate(G.params.splits):
    op = Operation("underfolder pipelime check")
    op.IN.input_folders = [G.params.out / S["name"]]
    op.OUT.output_folder = G.params.checked / S["name"]
