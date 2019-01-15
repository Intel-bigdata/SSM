import argparse
import timeout_decorator
import unittest
from util import *


class TestStressDR(unittest.TestCase):

    @timeout_decorator.timeout(seconds=60)
    def test_sync_rule(self):
        file_paths = []
        cids = []
        # create a directory with random name
        source_dir = TEST_DIR + random_string() + "/"
        # create random files in the above directory
        for i in range(MAX_NUMBER):
            file_path, cid = create_random_file_parallel(FILE_SIZE, source_dir)
            file_paths.append(file_path)
            cids.append(cid)
        wait_for_cmdlets(cids)

        # wait for DB sync
        time.sleep(5)
        rule_str = "file : every 1s | path matches " + \
            "\"" + source_dir + "*\" | sync -dest " + DEST_DIR
        rid = submit_rule(rule_str)
        start_rule(rid)
        # Status check
        while True:
            time.sleep(1)
            rule = get_rule(rid)
            if rule['numCmdsGen'] >= MAX_NUMBER:
                break
        cids = get_cids_of_rule(rid)
        failed = wait_for_cmdlets(cids)
        self.assertTrue(len(failed) == 0)
        time.sleep(5)
        stop_rule(rid)


if __name__ == '__main__':
    requests.adapters.DEFAULT_RETRIES = 5
    s = requests.session()
    s.keep_alive = False

    parser = argparse.ArgumentParser()
    parser.add_argument('-size', default='1MB',
                        help="size of file, Default Value 1MB.")
    parser.add_argument('-num', default='10000',
                        help="file num, Default Value 10000.")
    # To sync files to another cluster, please use "-dest hdfs://hostname:port/dest/"
    parser.add_argument('-dest', default='/dest/',
                        help="directory to store generated test set, DefaultValue: /dest/")
    parser.add_argument('unittest_args', nargs='*')
    args, unknown_args = parser.parse_known_args()
    sys.argv[1:] = unknown_args
    print "The file size for test is {}.".format(args.size)
    FILE_SIZE = convert_to_byte(args.size)
    print "The file number for test is {}.".format(args.num)
    MAX_NUMBER = int(args.num)
    print "The dest directory for test is {}.".format(args.dest)
    DEST_DIR = args.dest

    unittest.main()
