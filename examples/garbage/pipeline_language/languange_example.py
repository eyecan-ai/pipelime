from textx import metamodel_from_file


class Reader:

    def __init__(self, parent, folder):
        self.parent = parent
        self.folder = folder
        print("NEW READER", self.folder)


class Link:

    def __init__(
        self,
        parent,
        start,
        end
    ) -> None:
        pass


class Node:

    def __init__(self, parent, name, port) -> None:
        self.name = name
        self.port = port


class Port:

    def __init__(self, parent, name, arity) -> None:
        self.name = name
        self.arity = arity


class Arity:

    def __init__(self, parent, idx, name):
        self.parent = parent
        self.idx = idx
        self.name = name
        #print("ARITY CREATED", parent, idx, name)


class Declaration:

    def __init__(self, parent, name, operation) -> None:
        self.name = name
        self.operation = operation


class OperationSplitByQuery:

    def __init__(self, parent, name, query) -> None:
        self.name = name
        self.query = query


hello_meta = metamodel_from_file(
    'model.tx',
    classes=[Link, Node, Arity, Declaration, OperationSplitByQuery, Reader]
)

example_hello_model = hello_meta.model_from_file('example.txt')

for c in example_hello_model.rows:
    print(c, type(c))
    if isinstance(c, Link):
        link = c
        print("LINK", link.start, link.end)
