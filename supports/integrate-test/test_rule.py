import time
import unittest
from util import *


class TestRule(unittest.TestCase):
    def test_rule_access_count(self):
        # rule:
        # file : path matches "/test/*" and accessCount(1m) > 1 | allssd
        rule_str = "file : path matches " + \
            "\"/test/*\" and accessCount(1m) > 1 | allssd "
        rid = submit_rule(rule_str)
        start_rule(rid)
        file_path = create_random_file(10 * 1024 * 1024)
        # Activate rule
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

    def test_rule_age(self):
        # rule:
        # file : path matches "/test/*" and age > 4s | archive
        rule_str = "file : path matches \"/test/*\" and age > 4s | archive "
        rid = submit_rule(rule_str)
        start_rule(rid)
        file_path = create_random_file(10 * 1024 * 1024)
        # Activate rule
        # wait to trigger rule
        wait_for_cmdlet(read_file(file_path))
        time.sleep(5)
        # Statue check
        rule = get_rule(rid)
        self.assertTrue(rule['numCmdsGen'] > 0)
        delete_rule(rid)

    def test_rule_scheduled(self):
        # rule:
        # file: every 4s from now to now + 15s | path matches "/test/data*.dat" | onessd
        # From now to now + 15s
        rule_str = "file: " + \
            "every 4s from now to now + 15s |" + \
            " path matches " + \
            "\"/test/data*.dat\"" + \
            " | onessd "
        rid = submit_rule(rule_str)
        # Create two random files
        for _ in range(3):
            file_path = "/test/data" + \
                random_string() + ".dat"
            wait_for_cmdlet(create_file(file_path))
        # Activate rule
        start_rule(rid)
        # wait to trigger rule
        time.sleep(8)
        # Statue check
        rule = get_rule(rid)
        self.assertTrue(rule['numCmdsGen'] > 0)
        delete_rule(rid)


if __name__ == '__main__':
    unittest.main()
