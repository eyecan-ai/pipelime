import yaml
import json
from pathlib import Path
from typing import Sequence
import numpy as np
from PIL import Image, ImageDraw
import uuid
import imageio


class ToyDatasetGenerator(object):

    def __init__(self, palette=None):
        pass

    def generate_random_background(self, size, channels=3):
        return np.random.uniform(0, 255, (size[1], size[0], channels)).astype(np.uint8)

    def generate_random_object_2D(self, size, max_label=5):
        size = np.array(size)
        center = np.random.uniform(0.1 * size[0], 0.9 * size[0], (2,))
        random_size = np.random.uniform(size / 10, size / 2, (2,))
        top_left = np.array([center[0] - random_size[0] * 0.5, center[1] - random_size[1] * 0.5])
        bottom_right = np.array([center[0] + random_size[0] * 0.5, center[1] + random_size[1] * 0.5])
        width = random_size[0]
        height = random_size[1]
        label = np.random.randint(0, max_label + 1)

        return {
            'center': center,
            'size': random_size,
            'tl': top_left,
            'br': bottom_right,
            'w': width,
            'h': height,
            'label': label
        }

    def generate_2d_object_bbox(self, size, obj):
        center = obj['center']
        w, h = size
        return (obj['label'], center[0] / w, center[1] / h, obj['w'] / w, obj['h'] / h)

    def generate_2d_object_keypoints(self, size, obj):

        center = obj['center']
        tl = obj['tl']
        br = obj['br']
        obj_size = obj['size']
        w, h = size
        label = obj['label']

        orientation = 1 if obj_size[0] > obj_size[1] else 0
        scale = obj_size[0] / w

        kp0 = (label, center[0] / w, center[1] / h, scale, orientation, 0)
        kp1 = (label, tl[0] / w, tl[1] / h, scale, orientation, 1)
        kp2 = (label, br[0] / w, br[1] / h, scale, orientation, 2)

        return [kp0, kp1, kp2]

    def generate_2d_objects_images(self, size, objects):

        image = Image.fromarray(self.generate_random_background(size))
        mask = Image.new('L', size)
        instances = Image.new('L', size)

        for index, obj in enumerate(objects):
            label = obj['label']
            coords = tuple(obj['tl']), tuple(obj['br'])
            color = tuple(np.random.randint(0, 255, (3,)))

            ImageDraw.Draw(image).ellipse(coords, fill=color)
            ImageDraw.Draw(mask).ellipse(coords, fill=label + 1)
            ImageDraw.Draw(instances).ellipse(coords, fill=index + 1)

        return {
            'rgb': np.array(image),
            'mask': np.array(mask),
            'instances': np.array(instances)
        }

    def generate_image_sample(self, size, max_label=5, objects_number_range=[1, 5]):

        objects = []
        bboxes = []
        keypoints = []
        objects_number = np.random.randint(objects_number_range[0], objects_number_range[1])
        for n in range(objects_number):
            obj = self.generate_random_object_2D(size, max_label=max_label)
            box = self.generate_2d_object_bbox(size, obj)
            kps = self.generate_2d_object_keypoints(size, obj)
            objects.append(obj)
            bboxes.append(box)
            keypoints.extend(kps)

        data = self.generate_2d_objects_images(size, objects)
        data.update({
            'bboxes': bboxes,
            'keypoints': keypoints,
            'label': np.random.randint(max_label + 1),
            'id': str(uuid.uuid1())
        })
        return data

    @classmethod
    def generate_fake_dataset(cls, output_folder: str, size: int, image_size: Sequence[int], zfill: int = 5):

        output_folder = Path(output_folder)
        output_folder.mkdir(parents=True, exist_ok=True)

        generator = ToyDatasetGenerator()

        for idx in range(size):

            # Generate sample
            sample = generator.generate_image_sample([image_size, image_size])

            # Extracts metadata
            metadata = {
                'bboxes': sample['bboxes'],
                'keypoints': sample['keypoints'],
                'label': sample['label'],
                'id': sample['id']
            }

            metadata = json.loads(json.dumps(metadata))

            # Naming
            name = str(idx).zfill(zfill)
            image_name = f'{name}_image.jpg'
            mask_name = f'{name}_mask.png'
            instances_name = f'{name}_inst.png'
            metadata_name = f'{name}_metadata.yml'
            keypoints_name = f'{name}_keypoints.txt'
            bboxes_name = f'{name}_bboxes.npy'

            imageio.imwrite(str(output_folder / image_name), sample['rgb'])
            imageio.imwrite(str(output_folder / mask_name), sample['mask'])
            imageio.imwrite(str(output_folder / instances_name), sample['instances'])
            np.savetxt(str(output_folder / keypoints_name), sample['keypoints'])
            np.save(str(output_folder / bboxes_name), sample['bboxes'])

            yaml.safe_dump(metadata, open(str(output_folder / metadata_name), 'w'))
