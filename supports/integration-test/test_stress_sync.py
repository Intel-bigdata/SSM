import argparse
import unittest
from util import *


DEST_DIR = "hdfs://localhost:9000/dest"


class TestStressDR(unittest.TestCase):
    def test_sync_rule(self):
        # file : every 1s | path matches "/1MB/*" | sync -dest
        # file_path = create_random_file(10 * 1024 * 1024)
        # submit rule
        file_paths = []
        cids = []
        # create a directory with random name
        source_dir = TEST_DIR + random_string() + "/"
        # create random files in the above directory
        for i in range(MAX_NUMBER):
            file_paths.append(create_random_file_parallel(FILE_SIZE,
                                                          source_dir)[0])
        time.sleep(1)
        rule_str = "file : every 1s | path matches " + \
            "\"" + source_dir + "*\" | sync -dest " + DEST_DIR
        rid = submit_rule(rule_str)
        # Activate rule
        start_rule(rid)
        # Submit read action to trigger rule
        # Read three times
        time.sleep(1)
        # Statue check
        while True:
            time.sleep(1)
            rule = get_rule(rid)
            if rule['numCmdsGen'] >= MAX_NUMBER:
                break
        time.sleep(5)
        delete_rule(rid)
        # delete all random files
        for i in range(MAX_NUMBER):
            cids.append(delete_file(file_paths[i]))
        wait_for_cmdlets(cids)


if __name__ == '__main__':
    requests.adapters.DEFAULT_RETRIES = 5
    s = requests.session()
    s.keep_alive = False

    parser = argparse.ArgumentParser()
    parser.add_argument('-size', default='1MB')
    parser.add_argument('-num', default='10000')
    parser.add_argument('unittest_args', nargs='*')
    args, unknown_args = parser.parse_known_args()
    sys.argv[1:] = unknown_args
    print "The file size for test is {}.".format(args.size)
    FILE_SIZE = convert_to_byte(args.size)
    print "The file number for test is {}.".format(args.num)
    MAX_NUMBER = int(args.num)

    unittest.main()
