def final_validation(**kwargs):
    from pathlib import Path

    output_folder = Path(kwargs.get("OUTPUT_FOLDER", None))
    assert len(list(output_folder.glob("*"))) == 3
