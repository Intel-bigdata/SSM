import unittest
import time
from util import *


# 1MB
FILE_SIZE = 1024 * 1024
DEST_DIR = "hdfs://localhost:9000/dest"


class TestStressDR(unittest.TestCase):
    def test_sync_rule_10000(self):
        # file : every 1s | path matches "/1MB/*" | sync -dest
        # file_path = create_random_file(10 * 1024 * 1024)
        # submit rule
        max_number = 10000
        file_paths = []
        cids = []
        # create random directory
        source_dir = TEST_DIR + random_string() + "/"
        # create 10K random files in random directory
        for i in range(max_number):
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
            if rule['numCmdsGen'] >= max_number:
                break
        time.sleep(5)
        delete_rule(rid)
        # delete all random files
        for i in range(max_number):
            cids.append(delete_file(file_paths[i]))
        wait_for_cmdlets(cids)

    def test_sync_rule_100000(self):
        # file : every 1s | path matches "/1MB/*" | sync -dest
        # file_path = create_random_file(10 * 1024 * 1024)
        # submit rule
        max_number = 100000
        file_paths = []
        cids = []
        # create random directory
        source_dir = TEST_DIR + random_string() + "/"
        # create 10K random files in random directory
        for i in range(max_number):
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
            if rule['numCmdsGen'] >= max_number:
                break
        time.sleep(5)
        delete_rule(rid)
        # delete all random files
        for i in range(max_number):
            cids.append(delete_file(file_paths[i]))
        wait_for_cmdlets(cids)


if __name__ == '__main__':
    requests.adapters.DEFAULT_RETRIES = 5
    s = requests.session()
    s.keep_alive = False
    unittest.main()
