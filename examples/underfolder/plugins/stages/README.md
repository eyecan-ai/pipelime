# Examples of UnderfolderStagePlugin

An **UnderfolderPlugin** is a generic logic executed at the end of the Underfolder constructor.

The **UnderfolderStagePlugin**, for example, is responsible for adding custom stages to each Sample before the __getitem__ method is called.


A plugin is created with a *private* root file in the Undefolder. Root files starting with `_` are considered as *private* and reserved for guys like plugins. The **UnderfolderStagePlugin**, for example, is spawned with:

```
_underfolder_stage.yml
```

The plugin name is simply the key name without the `_` prefix and the plugin data is the content of the file itself. 

In this specific case the plugin data is a serialization of a generic **StageCompose** mapping together multiple **SampleStage**.

## example_stages.py

In this example is shown how to inject a custom stage into Underfolder as a plugin. The custom stage is a stage which add a 'result' key to each sample where the value is a function of sample["value"]:

$sample["result"] = sample["value"] \times a + b$ 

launch with:

```
python example_stages.py
```

⚠️⚠️ Pay attention ⚠️⚠️! If you use a custom stage class, like in this example, ensure that the class is imported before the plugin is loaded. **SampleStage** are **Spooks** so their registration to the **SampleStage** factory happens in metaclass declaration.