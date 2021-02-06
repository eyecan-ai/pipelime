from operator import mul, sub
import graphkit
from graphkit import compose, operation
# Computes |a|^p.
from pipelime.sequences.operations import OperationShuffle, OperationSum


def abspow(a, p):
    c = abs(a) ** p
    return c


# Compose the mul, sub, and abspow operations into a computation graph.
graph = compose(name="graph")(
    operation(name="l1", needs=["a", "b"], provides=["ab"])(OperationSum()),
    operation(name="l2", needs=["a", "ab"], provides=["aab"])(OperationSum()),
    operation(name="l3", needs=["aab"], provides=["out"])(OperationShuffle())
)

# Run the graph and request all of the outputs.
out = graph({'a': 2, 'b': 5})

# Prints "{'a': 2, 'a_minus_ab': -8, 'b': 5, 'ab': 10, 'abs_a_minus_ab_cubed': 512}".
print(out)

# Run the graph and request a subset of the outputs.
out = graph({'a': 2, 'b': 5}, outputs=["a_minus_ab"])

# Prints "{'a_minus_ab': -8}".
print(out)
