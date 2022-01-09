from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.samples import SamplesSequence
from pipelime.sequences.writers.filesystem import UnderfolderWriter

reader = UnderfolderReader(
    folder="../../tests/sample_data/datasets/underfolder_minimnist"
)

output_samples = []
for sample in reader:
    sample = sample.copy()

    sample["info"] = sample["metadatay"]

    del sample["metadatay"]
    del sample["metadata"]
    del sample["image_mask"]
    del sample["image_maskinv"]
    del sample["label"]
    del sample["points"]

    sample["categories"] = {
        "main": "category_odd" if sample.id % 2 == 1 else "category_even",
        "others": ["alpha", "beta", "gamma"]
        if sample.id % 2 == 1
        else ["delta", "epsilon", "zeta"],
    }
    output_samples.append(sample)


extensions_map = reader.get_reader_template().extensions_map
extensions_map.update({"categories": "yml", "info": "yml"})
root_files_keys = reader.get_reader_template().root_files_keys

writer = UnderfolderWriter(
    folder="../../tests/sample_data/datasets/underfolder_minimnist_queries",
    extensions_map=extensions_map,
    root_files_keys=root_files_keys,
)


writer(SamplesSequence(samples=output_samples))
