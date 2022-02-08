# Piper: Compile

A DAG file can contains placeholders and repetions blocks. The compile procedure will replace the placeholders with the values picked from the configuration file and replace the repetions blocks with valid values.

From this folder execute the following command:

```
pipelime piper compile -i dag.yml -p params.yml
```

This command will print the compiled dag file to stdout. The compiled dag file can be saved to a file (**$COMPILED_FILE**):

```
pipelime piper compile -i dag.yml -p params.yml -o $COMPILED_FILE
```

If you try to launch the compile tool without providing a configuration file, the tool will fail due to missing replacement for the placeholders:

```
pipelime piper compile -i dag.yml
```
