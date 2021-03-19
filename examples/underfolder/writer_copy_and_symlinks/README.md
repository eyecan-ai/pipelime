# UnderfolderWriter: copy samples and symlinks

How to pevent a UnderfloderWriter from Reading/Writing data if samples are intact
FileSystemSample (not manipulated).

## `copy_files`

```python
UnderfolderWriter(
    folder=writer_folder,
    copy_files=True
)
```

sets `copy_files=True` to speedup writer. Intact samples (not manipulated after UnderfolderReader) are
just copied to destination folder.

## `use_symlinks`

```python
UnderfolderWriter(
    folder=writer_folder,
    copy_files=True,
    use_symlinks=True
)
```

sets `copy_files=True` and `use_symlinks=True` to further speedup writer and prevent memory consumption
if your writer output is meant to be a transient dataset.
