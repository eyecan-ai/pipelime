def final_validation(**kwargs):
    from pipelime.sequences.readers.filesystem import UnderfolderReader

    final_folder = kwargs.get("FINAL_FOLDER", None)
    size = kwargs.get("SIZE", None)
    assert final_folder is not None
    assert size is not None

    reader = UnderfolderReader(folder=final_folder)
    assert len(reader) == (size * 3) // 2

    suffixes_set = set()
    for sample in reader:
        print(list(sample.keys()))
        print(sample["metadata"])
        suffixes_set.add(sample["metadata"]["suffix"])

    assert len(suffixes_set) == 3
