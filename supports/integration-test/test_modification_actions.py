import argparse
import unittest
from util import *


class TestMover(unittest.TestCase):

    def test_append(self):
        file_path = create_random_file(FILE_SIZE)
        cid = submit_cmdlet("append -file " + file_path + " -length 1024")
        cmd = wait_for_cmdlet(cid)
        self.assertTrue(cmd['state'] == "DONE", "Test failed for append action.")

    def test_concat(self):
        file_paths = ""
        for i in range(MAX_NUMBER):
            file_paths += create_random_file(FILE_SIZE)
            if i != MAX_NUMBER - 1:
                file_paths += ","
        dest_path = TEST_DIR + random_string()
        cid = submit_cmdlet("concat -file " + file_paths + " -dest " + dest_path)
        cmd = wait_for_cmdlet(cid)
        self.assertTrue(cmd['state'] == "DONE", "Test failed for concat action on multiple files.")

    def test_merge(self):
        file_paths = ""
        for i in range(MAX_NUMBER):
            file_paths += create_random_file(FILE_SIZE)
            if i != MAX_NUMBER - 1:
                file_paths += ","
        dest_path = TEST_DIR + random_string()
        cid = submit_cmdlet("merge -file " + file_paths + " -dest " + dest_path)
        cmd = wait_for_cmdlet(cid)
        self.assertTrue(cmd['state'] == "DONE", "Test failed for merge action on multiple files.")

    def test_truncate(self):
        file_path = create_random_file(FILE_SIZE)
        cid = submit_cmdlet("truncate -file " + file_path + " -length " + str(random.randint(0, FILE_SIZE)))
        cmd = wait_for_cmdlet(cid)
        self.assertTrue(cmd['state'] == "DONE", "Test failed for truncate action.")

    def test_truncate0(self):
        file_path = create_random_file(FILE_SIZE)
        cid = submit_cmdlet("truncate0 -file " + file_path)
        cmd = wait_for_cmdlet(cid)
        self.assertTrue(cmd['state'] == "DONE", "Test failed for truncate0 action.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-size', default='64MB',
                        help="size of file, Default Value 64MB.")
    parser.add_argument('-num', default='10',
                        help="file num, Default Value 10.")
    parser.add_argument('unittest_args', nargs='*')
    args, unknown_args = parser.parse_known_args()
    sys.argv[1:] = unknown_args
    print "The file size for test is {}.".format(args.size)
    FILE_SIZE = convert_to_byte(args.size)
    print "The file number for concat and merge test is {}.".format(args.num)
    MAX_NUMBER = int(args.num)

    unittest.main()