# How to setup a versioned dataset using remotes

Datasets are usually full of binary data, eg, images, which are not
suitable for versioning. Luckily, Pipelime can replace such heavy data with small text
files storing references to external data lakes, so that:
* the full dataset can be easily versioned and shared
* the data is automatically downloaded when reading the item

To setup a versioned dataset, first you need a data lake, eg, a shared folder or a
S3-compatible data server. Then, prepare your data in Underfolder format and run the
`remote_add` operation:

```
pipelime underfolder remote_add -i path/to/input -o path/to/output -r file://localhost/path/to/root/bucket-name -r s3://172.218.0.0:9000/bucket-name?access_key=user:secret_key=pwd:secure_connection=False -k image -k mask --hardlink
```

The remotes are listed as url with optional init arguments as query data. Note that the
S3 interface can load the access and secret keys from environment variables or aws
config files as well. The output dataset will have the listed keys, ie, `image` and `mask`,
replaced with `.remote` text files referencing to both remotes.
Similarly, to remove a remote from a remote list, just call `remote_remove`:

```
pipelime underfolder remote_remove -i path/to/input -o path/to/output -r s3://172.218.0.0:9000/bucket-name -k image -k mask --hardlink
```

Note that the file is **not** removed on the remote.

Reading a dataset with data on remotes is straightforward, provided that at list one remote is accessible. Then, actual data can be easily cached:

```python
reader = UnderfolderReader("path/to/dataset")
filtered_data = SamplesSequence(
    reader,
    StageKeysFilter(("image", "mask", "label")),
)
cached_seq = CachedSamplesSequence(
    filtered_data,
    CachedSamplesSequence.PersistentCachePolicy("path/to/cache_folder"),
    ("image", "mask", "label"),
)
```

The first iteration over `cached_seq` will _transparently_ download the data from the remote, then subsequent calls will just pick the cached binary files. This way, a simple git-clone
of the data repository can be enough to share any binary dataset.
