import unittest
from util import *

# 1MB
FILE_SIZE = 1024 * 1024


class TestSmallFile(unittest.TestCase):

    def test_small_file_compact(self):
        max_number = 500
        file_paths = []
        # create random directory in ssmtest
        source_dir = TEST_DIR + random_string() + "/"
        # create 500 random files in random directory
        for i in range(max_number):
            file_paths.append(create_random_file_parallel(FILE_SIZE,
                                                          source_dir))
        time.sleep(2)
        # compact rule
        rule_str = "file : path matches " + \
            "\"" + source_dir + "*\" | compact"
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
            cids.append(delete_file(file_path[i]))
        wait_for_cmdlets(cids)


if __name__ == '__main__':
    unittest.main()
