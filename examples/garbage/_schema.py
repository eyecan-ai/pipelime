from schema import And
import numpy as np

# schema = {
#     'image': And(
#         np.ndarray,
#         lambda x: len(x.shape) >= 3
#     ),
#     'metadata': {
#         any: object,
#         'counter': int
#     },
#     any: object
# }
schema = {
    'image': object,
    'metadata': object,
    any: object
}
