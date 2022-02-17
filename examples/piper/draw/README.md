# Piper: Draw

A DAG file can be drawn using a specific drawing backend. The backend can be specified using the **-b** option (**-help** to list avaiable backends). 

## Draw Graphviz version to PDF 

In order to generate a PDF representation of the DAG using [Graphviz](https://graphviz.org/) (or better [PyGraphviz](https://pygraphviz.github.io/)), use the following command:

```
pipelime piper draw -i drawable_dag.yml -b graphviz -o /tmp/drawable_dag.pdf
```

To auto-open file after execution use the following command:

```
pipelime piper draw -i drawable_dag.yml -b graphviz -o /tmp/drawable_dag.pdf --open
```

Try also **PNG** ...


## Draw Mermaid to Markdown

In order to generate a Markdown representation of the DAG with [Mermaid](https://github.com/mermaid-js/mermaid) syntax, use the following command:

```
pipelime piper draw -i drawable_dag.yml -b mermaid -o /tmp/drawable_dag.md --open
```

Try also **PNG** ...
