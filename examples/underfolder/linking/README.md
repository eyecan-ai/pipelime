# Examples of UnderfolderLinksPlugin

An **UnderfolderPlugin** is a generic logic executed at the end of the Underfolder constructor.

The **UnderfolderLinksPlugin**, for example, is responsible for creating links to other Underfolder. 
Recursively, each Underfolder loads its linked Underfolder and merge its samples with the samples of the linked one (*i.e* fuse sample keys).

A plugin is created with a *private* root file in the Undefolder. Root files starting with `_` are considered as *private* and reserved for guys like plugins. The **UnderfolderLinksPlugin**, for example, is spawned with:

```
_underfolder_links.yml
```

The plugin name is simply the key name without the `_` prefix and the plugin data is the content of the file itself. 

In this specific case the plugin data is a plain list of folders of other Underfolder to link.


**No loop are allowed**! Loops are detected and an exception is raised.


## example_link_incremental_update.py

In this example is shown how to use links to create *incremenal update* of the samples. If, for example, you have a stage which transform a sample into another one (*e.g.* a labeling GUI adding labels to the samples, an AI model adding prediction etc..) and you want to use this content in the next stage, you can use the links to create incremental update of the dataset, instead of overwriting previous underfolder or copying it to the next stage.

The example read an Underfolder, add some keys to each sample, and stores
only the added content as a new Underfolder with a link to the previous one. Creating an 
**UnderfolderReader** on the last folder will load a single dataset with all data 
together.

```
python example_link_incremental_update.py
```


## example_link_tree.py

In this example is shown a mid-complex example of nested underfolder like these:

```
            ┌────┐
        ┌───►    ├──┐
        │   │B   │  │   ┌────┐
        │   └────┘  ├───►    │
        │           │   │E   │
┌────┐  │   ┌────┐  │   └────┘
│    ├──┼───►    │  │
│A   │  │   │C   │  │
└────┘  │   └────┘  │   ┌────┐
        │           └───►    │
        │   ┌────┐      │F   │
        └───►    │      └────┘
            │D   │
            └────┘
```

The **UnderfolderReader** starting from the **A** folder will load all the tree of Underfolder and merge the samples together. The example will plot also the **NetworkX**
graph of the tree. The Graph is automatically generated and saved in the plugin data.

```
python example_link_tree.py
```
