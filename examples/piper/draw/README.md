# Piper: Draw

A DAG file can be drawn using a specific drawing backend. The backend can be specified using the **-b** option (**-help** to list avaiable backends). 

From this folder execute the following command:

```
pipelime piper draw -i drawable_dag.yml
```

This command will draw and display the piper dag using opencv. So save the image to a file (**$OUTPUT_FILE**):

```
pipelime piper draw -i drawable_dag.yml -o $OUTPUT_FILE
```
