import argparse
import unittest
from util import *


# To run this script, we must install hadoop and enable S3 support in hadoop
DEST_DIR = "s3a://xxxctest/"


class TestS3(unittest.TestCase):

    # copy to S3
    def test_S3(self):
        file_paths = []
        cids = []
        # create random directory
        source_dir = "/" + random_string() + "/"
        # create 1K random files in random directory
        for i in range(MAX_NUMBER):
            file_paths.append(
                create_random_file_parallel(FILE_SIZE, source_dir)[0])
        time.sleep(1)

        # submit action
        for i in range(MAX_NUMBER):
            cids.append(copy_file_to_S3(source_dir + file_paths[i],
                                        DEST_DIR + file_paths[i]))

        failed_cids = wait_for_cmdlets(cids)
        self.assertTrue(len(failed_cids) == 0)

        # delete file from S3
        print "delete test file from S3"
        for i in range(MAX_NUMBER):
            subprocess.call("hadoop fs -rm " + DEST_DIR +
                            file_paths[i], shell=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-size', default='1MB')
    parser.add_argument('-num', default='100')
    parser.add_argument('unittest_args', nargs='*')
    args, unknown_args = parser.parse_known_args()
    sys.argv[1:] = unknown_args
    print "The file size for test is {}.".format(args.size)
    FILE_SIZE = convert_to_byte(args.size)
    print "The file number for test is {}.".format(args.num)
    MAX_NUMBER = int(args.num)

    unittest.main()
