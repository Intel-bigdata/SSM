import random
import time
import unittest
from util import *


class TestRule(unittest.TestCase):
    def test_rule_access_count(self):
        # Submit rule file : path matches "/test/*" and accessCount(1m) > 1 | allssd
        rule_str = "file : path matches " + \
            "\"/test/*\" and accessCount(1m) > 1 | allssd "
        rid = submit_rule(rule_str)
        start_rule(rid)
        file_path = TEST_FILES[random.randrange(len(TEST_FILES))]
        # Activate rule
        # Submit read action to trigger rule
        # Read twice
        cid_r1 = read_file(file_path)
        cid_r2 = read_file(file_path)
        cid_r3 = read_file(file_path)
        wait_for_cmdlet(cid_r1)
        wait_for_cmdlet(cid_r2)
        wait_for_cmdlet(cid_r3)
        time.sleep(15)
        # Statue check
        rule = get_rule(rid)
        self.assertTrue(rule['numCmdsGen'] > 0)
        delete_rule(rid)

    def test_rule_age(self):
        # Submit rule file : path matches "/test/*" and age > 4s | archive 
        rule_str = "file : path matches \"/test/*\" and age > 4s | archive "
        rid = submit_rule(rule_str)
        start_rule(rid)
        file_path = TEST_FILES[random.randrange(len(TEST_FILES))]
        # Activate rule
        # wait to trigger rule
        # Read twice
        wait_for_cmdlet(read_file(file_path))
        time.sleep(5)
        # Statue check
        rule = get_rule(rid)
        self.assertTrue(rule['numCmdsGen'] > 0)
        delete_rule(rid)

    def test_rule_scheduled(self):
        # Submit rule file: every 4s from now to now + 15s | path matches "/test/data*.dat" | onessd
        # From current to current + 10s
        rule_str = "file: " + \
            "every 4s from now to now + 15s" + \
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

    def test_rule_stress(self):
        # Add 100000 different rules
        pass


if __name__ == '__main__':
    unittest.main()
