from pipelime.sequences.samples import SamplesSequence
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.proxies import (
    FilteredSamplesSequence,
    ConcatSamplesSequence,
    SortedSamplesSequence,
    CachedSamplesSequence,
)
from pipelime.sequences.stages import StageKeysFilter, StageAugmentations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter_ns

#
# We are going to create this pipeline:
#
#         dataset
#     ______|______
#    |             |
# reader_a      reader_b
#    |             |
# filter_odd    filter_even
#  + augment     + augment
#    |             |
#    |          sort_descending
#    |_____________|
#           |
#         concat
#           |
#         cache
#
# Then we will go through all the samples multiple times, measuring time performances.
#

dataset = str(
    Path(__file__).resolve().parent
    / "../../../tests/sample_data/datasets/underfolder_minimnist"
)

# composing with a helper sequence to set a Stage on top of any serialized Stage
reader_a = SamplesSequence(
    UnderfolderReader(dataset),
    StageKeysFilter(("image", "image_mask", "image_maskinv", "label")),
)
reader_b = SamplesSequence(
    UnderfolderReader(dataset),
    StageKeysFilter(("image", "image_mask", "image_maskinv", "label")),
)

augmentation = {
    "__version__": "1.0.3",
    "transform": {
        "__class_fullname__": "Compose",
        "p": 1.0,
        "transforms": [
            {
                "__class_fullname__": "Resize",
                "always_apply": False,
                "p": 1,
                "height": 1024,
                "width": 1024,
                "interpolation": 1,
            },
            {"__class_fullname__": "HorizontalFlip", "always_apply": False, "p": 1},
            {
                "__class_fullname__": "ShiftScaleRotate",
                "always_apply": False,
                "p": 1.0,
                "shift_limit_x": [-0.2, 0.2],
                "shift_limit_y": [-0.2, 0.2],
                "scale_limit": [-0.6, 0.6000000000000001],
                "rotate_limit": [-180, 180],
                "interpolation": 1,
                "border_mode": 4,
                "value": None,
                "mask_value": None,
            },
            {
                "__class_fullname__": "RandomBrightnessContrast",
                "always_apply": False,
                "p": 1,
                "brightness_limit": [-0.2, 0.2],
                "contrast_limit": [-0.2, 0.2],
                "brightness_by_max": True,
            },
        ],
        "bbox_params": None,
        "keypoint_params": None,
        "additional_targets": {},
    },
}

filter_odd = FilteredSamplesSequence(
    reader_a,
    lambda x: int(x["label"]) % 2 == 1,
    StageAugmentations(
        augmentation,
        targets={"image": "image", "image_mask": "mask", "image_maskinv": "mask"},
    ),
)
filter_even = FilteredSamplesSequence(
    reader_b,
    lambda x: int(x["label"]) % 2 == 0,
    StageAugmentations(
        augmentation,
        targets={"image": "image", "image_mask": "mask", "image_maskinv": "mask"},
    ),
)

sort_descending = SortedSamplesSequence(filter_even, lambda x: -int(x["label"]))

concat = ConcatSamplesSequence([filter_odd, sort_descending])

# create a temp dir that will be removed at exit
with TemporaryDirectory() as tmp_cache_folder:
    cached_seq = CachedSamplesSequence(
        concat,
        CachedSamplesSequence.PersistentCachePolicy(tmp_cache_folder),
        ("image", "image_mask", "image_maskinv", "label"),
    )

    print("*** Traversing the whole dataset:")
    for i, s in enumerate(cached_seq):
        print(f"[{i:03d}] " + ", ".join(s.keys()) + f" - {s['label']}")

    # PERFORMANCE UTILITIES
    def time_perf(sseq):
        tic = perf_counter_ns()
        for s in sseq:
            for k in s.keys():
                _ = s[k]
        for s in reversed(sseq):
            for k in s.keys():
                _ = s[k]
        toc = perf_counter_ns()
        return (toc - tic) * 1e-6

    def double_run_perf(sseq, lbl, numbers=5):
        for i in range(numbers):
            elapsed = time_perf(sseq)
            print(f"** {lbl} (run #{i}): {elapsed} ms")

    print("*** Traversing dataset in both directions for performance evaluation")

    #######################
    # SIMPLE CACHING TO DISK
    #######################
    cached_seq = CachedSamplesSequence(
        concat,
        CachedSamplesSequence.PersistentCachePolicy(tmp_cache_folder, clear_cache=True),
        ("image", "image_mask", "image_maskinv", "label"),
    )
    double_run_perf(cached_seq, "SIMPLE DISK CACHE")

    #######################
    # SHARING CACHE DATA
    #######################
    cached_seq = CachedSamplesSequence(
        concat,
        CachedSamplesSequence.PersistentCachePolicy(tmp_cache_folder),
        ("image", "image_mask", "image_maskinv", "label"),
    )
    double_run_perf(cached_seq, "SHARED CACHE")

    #######################
    # CACHING WITH MEMORY BUFFER
    #######################
    cached_seq = CachedSamplesSequence(
        concat,
        CachedSamplesSequence.PersistentCachePolicy(
            tmp_cache_folder, clear_cache=True, max_buffer_size=len(concat) // 2
        ),
        ("image", "image_mask", "image_maskinv", "label"),
    )
    double_run_perf(cached_seq, "HALF MEMORY BUFFER")

    cached_seq = CachedSamplesSequence(
        concat,
        CachedSamplesSequence.PersistentCachePolicy(
            tmp_cache_folder, clear_cache=True, max_buffer_size=len(concat)
        ),
        ("image", "image_mask", "image_maskinv", "label"),
    )
    double_run_perf(cached_seq, "FULL MEMORY BUFFER")
