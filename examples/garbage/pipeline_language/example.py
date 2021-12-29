class Port:
    pass

    def __le__(self, x):
        pass


class Node:
    def __init__(self, **kwargs) -> None:
        self.input = [Port()] * 100
        self.output = [Port()] * 100

    def __ge__(self, x):
        print(self, "->", x)


alfa = Node(folder="/tmp")
beta = Node(folder="/tmp")
sum = Node()

alfa > sum.input[0]
