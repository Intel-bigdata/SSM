import unittest
import time
from util import *

DEST_DIR = "hdfs://datanode3:9000/dest"


class TestStressDR(unittest.TestCase):
    def test_rule_access_count(self):
        # file : every 1s | path matches "/1MB/*" | sync -dest
        # file_path = create_random_file(10 * 1024 * 1024)
        # submit rule
        source_dir = "/1MB/"
        rule_str = "file : every 1s | path matches " + \
            "\"" + source_dir + "*\" | sync -dest " + DEST_DIR
        rid = submit_rule(rule_str)
        # Activate rule
        start_rule(rid)
        # Submit read action to trigger rule
        # Read three times
        cmds = []
        cmds.append(read_file(file_path))
        cmds.append(read_file(file_path))
        cmds.append(read_file(file_path))
        wait_for_cmdlets(cmds)
        time.sleep(15)
        # Statue check
        rule = get_rule(rid)
        self.assertTrue(rule['numCmdsGen'] > 0)
        delete_rule(rid)


if __name__ == '__main__':
    unittest.main()
