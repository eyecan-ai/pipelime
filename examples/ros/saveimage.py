from sensor_msgs.msg import Image
from cv_bridge import CvBridge, CvBridgeError
import cv2
import sys
import rospy
import tf
import numpy as np
import transforms3d
from pipelime.sequences.writers.filesystem import UnderfolderWriter
from pipelime.sequences.samples import PlainSample



# extensions = {
#             'image': 'jpg',
#             'image_mask': 'png',
#             'image_maskinv': 'png',
#             'label': 'txt',
#             'metadata': 'json',
#             'metadatay': 'yml',
#             'points': 'txt',
#             'numbers': 'txt',
#             'pose': 'txt',
#             'cfg': 'yml',
#             'tracepen': 'txt'
#         }

## Example of a writer
# self.writer = UnderfolderWriter(
#     folder=self.save_path_underfolder,
#     extensions_map=extensions
# )



def save_file(self, id_sample, name_sample, start_tf_transform, end_tf_transform, writer, image):
        """ Saves image and positing of an endpoint using the pipelime
        ​
        :param id_sample: id of the sample, defaults to 0000
        :type id_sample: string
        :param name_sample: name of the sample to get the file extension, defaults to pose
        :type name_sample: string
        :param tf_transform: transform to look up for the sample, defaults to ee_link
        :type tf_transform: string      
        :param writer: pipelime writer with extensions and folders
        :type writer: UnderfolderWriter     ​
        :param image: image to be saved as sample in rgb8 format
        :type image: sensor_msgs.msg.Image
        """

        # Pose of the flange upon the base link - transformation from /ee_link to /camera_link is necessary afterwards
        try:
            (trans,rot) = self.tf_listener.lookupTransform(str(start_tf_transform), '/' + str(end_tf_transform), rospy.Time(0))

            # Transform the trans and rot into an homogeneous matrix using transforms3d
            sample_q = np.array([rot[3], rot[0], rot[1], rot[2]])
            sample_hommat = transforms3d.quaternions.quat2mat(sample_q)
            sample_trans = np.array([[trans[0]], [trans[1]], [trans[2]]])
            sample_hommat = np.hstack((sample_hommat,sample_trans))
            sample_hommat = np.vstack((sample_hommat, np.array([0,0,0,1])))

            samples = []
            samples.append(PlainSample(data = {str(name_sample): sample_hommat}, id=id_sample))

            # Saves the image only if the sample regards the transformation from the ee_link
            cv2_img = self.bridge.imgmsg_to_cv2(image, "rgb8")
            samples.append(PlainSample(data = {str('image'): cv2_img}, id=id_sample))

            writer(samples)       
       
        except (tf.LookupException, tf.ConnectivityException, tf.ExtrapolationException, rospy.ROSInterruptException):
            rospy.logerr('Saving sample failed or TF lookup did not work')