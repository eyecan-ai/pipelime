# Underfolder Stream is used to read/write an underfolder interactively

The Underfolder Stream has the following architecture:

```
             ┌───────────────────┐  ┌─────────────┐
     ┌───────►                   │  │             │
     │       │ UnderfolderReader ├──►             ├──────► get_data
┌────┴────┐  │                   │  │             │
│  Disk   │  └───────────────────┘  │  Dataset    │
│ ------  │                         │  Stream     │
│ ------  │  ┌───────────────────┐  │             │
└────▲────┘  │                   │  │             ◄─────── set_data
     │       │ UnderfolderWriter ◄──┤             │
     └───────┤                   │  │             │
             └───────────────────┘  └─────────────┘
```


The following examples creates a Stream on a Minimnist underfolder. Resize images and
pushes new infos in metadata:

```sh
python streams_example.py
```
