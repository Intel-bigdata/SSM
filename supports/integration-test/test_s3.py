import argparse
import unittest
from util import *


# To run this script, HDFS should be installed locally and configured for S3 support.
# Please refer to s3-suppport.md in SSM repo's doc directory.

DEST_DIR = "s3a://xxxctest"


class TestS3(unittest.TestCase):

    # copy to S3
    def test_s3(self):
        file_paths = []
        cids = []
        # create random directory
        source_dir = TEST_DIR + random_string() + "/"
        for i in range(MAX_NUMBER):
            file_path, cid = create_random_file_parallel(FILE_SIZE, source_dir)
            file_paths.append(file_path)
            cids.append(cid)
        failed_cids = wait_for_cmdlets(cids)
        self.assertTrue(len(failed_cids) == 0)

        # wait for DB sync
        time.sleep(5)
        # submit actions
        cids = []
        for i in range(MAX_NUMBER):
            cids.append(copy_file_to_s3(file_paths[i],
                                        DEST_DIR + file_paths[i]))
        failed_cids = wait_for_cmdlets(cids)
        self.assertTrue(len(failed_cids) == 0)

        # delete files from S3
        print "delete test file from S3"
        subprocess.call("hadoop fs -rm -r " + DEST_DIR +
                            TEST_DIR, shell=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-size', default='1KB',
                        help="size of file, Default Value 1KB.")
    parser.add_argument('-num', default='10',
                        help="file num, Default Value 10.")
    parser.add_argument('unittest_args', nargs='*')
    args, unknown_args = parser.parse_known_args()
    sys.argv[1:] = unknown_args
    print "The file size for test is {}.".format(args.size)
    FILE_SIZE = convert_to_byte(args.size)
    print "The file number for test is {}.".format(args.num)
    MAX_NUMBER = int(args.num)

    unittest.main()
