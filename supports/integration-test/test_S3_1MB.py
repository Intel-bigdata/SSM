import unittest
import os
from util import *


FILE_SIZE = 1024 * 1024
DEST_DIR = "s3a://xxxctest/"

#To run this script, we must install hadoop and enable S3 support in hadoop


class Test_S3_1MB(unittest.TestCase):

    # move to S3
    def test_S3(self):
        max_number = 100
        file_paths = []
        cids = []

        #create random directory
        source_dir  = "/" + random_string() + "/"
        #create 1K random files in random directory
        for i in range(max_number):
            file_paths.append(
                create_random_file_parallel_return_file_name(source_dir, FILE_SIZE))
        time.sleep(1)

        # submit action
        for i in range(max_number):
            cids.append(copy_file_to_S3(source_dir + file_paths[i], DEST_DIR + file_paths[i]))

        failed_cids = wait_for_cmdlets(cids)
        self.assertTrue(len(failed_cids) == 0)

        #delete file from S3
        print "delete test file from S3"
        for i in range(max_number):
            subprocess.call("hadoop fs -rm " + DEST_DIR + file_paths[i], shell=True)



if __name__ == '__main__':
    unittest.main()
