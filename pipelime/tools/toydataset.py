import json
import uuid
from pathlib import Path
import numpy as np
from PIL import Image, ImageDraw
from typing import Callable, Tuple
from pipelime.filesystem.toolkit import FSToolkit
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.tools.progress import pipelime_track


class ToyDatasetGenerator(object):
    def __init__(self, palette=None):
        pass

    def generate_random_background(self, size, channels=3):
        return np.random.uniform(0, 255, (size[1], size[0], channels)).astype(np.uint8)

    def generate_random_object_2D(self, size, max_label=5):
        size = np.array(size)
        center = np.random.uniform(0.25 * size[0], 0.75 * size[0], (2,))
        random_size = np.random.uniform(size * 0.05, size * 0.24, (2,))
        top_left = np.array(
            [center[0] - random_size[0] * 0.5, center[1] - random_size[1] * 0.5]
        )
        bottom_right = np.array(
            [center[0] + random_size[0] * 0.5, center[1] + random_size[1] * 0.5]
        )
        diag = bottom_right - top_left

        kp0 = np.array([center[0], center[1] - random_size[1] * 0.5])
        kp1 = np.array([center[0], center[1] + random_size[1] * 0.5])

        width = random_size[0]
        height = random_size[1]
        label = np.random.randint(0, max_label + 1)

        return {
            "center": center,
            "size": random_size,
            "tl": top_left,
            "br": bottom_right,
            "diag": diag,
            "kp0": kp0,
            "kp1": kp1,
            "w": width,
            "h": height,
            "label": label,
        }

    def generate_2d_object_bbox(self, size, obj):
        center = obj["center"]
        w, h = size
        return (
            center[0] - obj["w"] * 0.5,
            center[1] - obj["h"] * 0.5,
            center[0] + obj["w"] * 0.5,
            center[1] + obj["h"] * 0.5,
            obj["label"],
        )

    def generate_2d_object_keypoints(self, size, obj):

        # center = obj['center']
        tl = obj["tl"]
        br = obj["br"]
        diag = obj["diag"]
        # obj_size = obj['size']
        w, h = size
        label = obj["label"]

        orientation = np.arctan2(diag[1], diag[0])
        orientation2 = np.arctan2(-diag[1], -diag[0])

        scale = 0.5 * np.linalg.norm(diag)

        # kp0 = (label, center[0] / w, center[1] / h, orientation, scale, 0)
        kp1 = (tl[0], tl[1], -orientation, scale, label, 1)
        kp2 = (br[0], br[1], -orientation2, scale, label, 2)

        return [kp1, kp2]

    def generate_2d_objects_images(self, size, objects):

        image = Image.fromarray(self.generate_random_background(size))
        mask = Image.new("L", size)
        instances = Image.new("L", size)

        for index, obj in enumerate(objects):
            label = obj["label"]
            coords = tuple(obj["tl"]), tuple(obj["br"])
            color = tuple(np.random.randint(0, 255, (3,)))

            # ImageDraw.Draw(image).ellipse(coords, fill=color)
            # ImageDraw.Draw(mask).ellipse(coords, fill=label + 1)
            # ImageDraw.Draw(instances).ellipse(coords, fill=index + 1)

            ImageDraw.Draw(image).rectangle(coords, fill=color)
            ImageDraw.Draw(mask).rectangle(coords, fill=label + 1)
            ImageDraw.Draw(instances).rectangle(coords, fill=index + 1)

        return {
            "rgb": np.array(image),
            "mask": np.array(mask),
            "instances": np.array(instances),
        }

    def generate_image_sample(self, size, max_label=5, objects_number_range=(1, 5)):

        objects = []
        bboxes = []
        keypoints = []
        objects_number = np.random.randint(
            objects_number_range[0], objects_number_range[1]
        )
        for n in range(objects_number):
            obj = self.generate_random_object_2D(size, max_label=max_label)
            box = self.generate_2d_object_bbox(size, obj)
            kps = self.generate_2d_object_keypoints(size, obj)
            objects.append(obj)
            bboxes.append(box)
            keypoints.extend(kps)

        rnd_bin = int(474)  # written as big-endian binary, equals to an image header
        data = self.generate_2d_objects_images(size, objects)
        data.update(
            {
                "bboxes": bboxes,
                "keypoints": keypoints,
                "label": np.random.randint(max_label + 1),
                "id": str(uuid.uuid1()),
                "bin": rnd_bin.to_bytes((rnd_bin.bit_length() + 7) // 8, "big"),
            }
        )
        return data

    @classmethod
    def generate_toy_dataset(
        cls,
        output_folder: str,
        size: int,
        image_size: int = 256,
        zfill: int = 5,
        suffix: str = "",
        as_underfolder: bool = False,
        max_label: int = 5,
        objects_number_range: Tuple[int, int] = (1, 5),
        progress_callback: Callable[[dict], None] = None,
    ):

        output_folder = Path(output_folder)
        if as_underfolder:
            output_folder = output_folder / UnderfolderReader.DATA_SUBFOLDER
        output_folder.mkdir(parents=True, exist_ok=True)

        generator = ToyDatasetGenerator()

        for idx in pipelime_track(range(size), track_callback=progress_callback):

            # Generate sample
            sample = generator.generate_image_sample(
                [image_size, image_size], max_label, objects_number_range
            )

            # Extracts metadata
            metadata = {
                f"bboxes{suffix}": sample["bboxes"],
                f"keypoints{suffix}": sample["keypoints"],
                f"label{suffix}": sample["label"],
                f"id{suffix}": sample["id"],
                f"bin{suffix}": int.from_bytes(sample["bin"], "big"),
                f"index{suffix}": idx,
                "suffix": suffix,
            }

            metadata = json.loads(json.dumps(metadata))

            # Naming
            name = str(idx).zfill(zfill)
            image_name = f"{name}_image{suffix}.png"
            mask_name = f"{name}_mask{suffix}.png"
            instances_name = f"{name}_inst{suffix}.png"
            metadata_name = f"{name}_metadata{suffix}.yml"
            metadataj_name = f"{name}_metadataj{suffix}.json"
            # metadatat_name = f"{name}_metadatat{suffix}.toml"
            keypoints_name = f"{name}_keypoints{suffix}.txt"
            keypointsp_name = f"{name}_keypointsp{suffix}.pickle"
            bboxes_name = f"{name}_bboxes{suffix}.npy"
            bin_name = f"{name}_bin{suffix}.bin"

            FSToolkit.store_data(str(output_folder / image_name), sample["rgb"])
            FSToolkit.store_data(str(output_folder / mask_name), sample["mask"])
            FSToolkit.store_data(
                str(output_folder / instances_name), sample["instances"]
            )
            FSToolkit.store_data(
                str(output_folder / keypoints_name), sample["keypoints"]
            )
            FSToolkit.store_data(
                str(output_folder / keypointsp_name), sample["keypoints"]
            )
            FSToolkit.store_data(str(output_folder / bboxes_name), sample["bboxes"])
            FSToolkit.store_data(str(output_folder / bin_name), sample["bin"])
            FSToolkit.store_data(str(output_folder / metadata_name), metadata)
            FSToolkit.store_data(str(output_folder / metadataj_name), metadata)
            # FSToolkit.store_data(str(output_folder / metadatat_name), metadata)
