import argparse
import timeout_decorator
import unittest
from util import *


class TestRule(unittest.TestCase):

    @timeout_decorator.timeout(seconds=60)
    def test_rule_access_count(self):
        # rule:
        # file : path matches "${file_path}" and accessCount(1m) > 1 | allssd
        file_path = create_random_file(10 * 1024 * 1024)
        # submit rule
        rule_str = "file : path matches \"" + file_path + "\" and accessCount(1m) > 1 | allssd "
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
        # Status check
        rule = get_rule(rid)
        while rule['numCmdsGen'] != 1:
            rule = get_rule(rid)
        cids = get_cids_of_rule(rid)
        failed = wait_for_cmdlets(cids)
        self.assertTrue(len(failed) == 0)
        stop_rule(rid)

    @timeout_decorator.timeout(seconds=60)
    def test_rule_age(self):
        # rule:
        # file : path matches "${file_path}" and age > 4s | archive
        file_path = create_random_file(10 * 1024 * 1024)
        # submit rule
        rule_str = "file : path matches \"" + file_path + "\" and age > 4s | archive "
        rid = submit_rule(rule_str)
        # Activate rule
        start_rule(rid)
        wait_for_cmdlet(read_file(file_path))
        # Status check
        rule = get_rule(rid)
        while rule['numCmdsGen'] != 1:
            rule = get_rule(rid)
        cids = get_cids_of_rule(rid)
        failed = wait_for_cmdlets(cids)
        self.assertTrue(len(failed) == 0)
        stop_rule(rid)

    @timeout_decorator.timeout(seconds=60)
    def test_rule_scheduled(self):
        # rule:
        # file: every 4s from now to now + 15s | path matches "${TEST_DIR}${prefix}-*" | onessd
        # From now to now + 15s
        # Create 3 random files
        prefix = random_string()
        for _ in range(3):
            file_path = TEST_DIR + prefix + "-" + random_string()
            wait_for_cmdlet(create_file(file_path, 10 * 1024 * 1024))
        # submit rule
        rule_str = "file: " + "every 4s from now to now + 15s |" + \
            " path matches " + "\"" + TEST_DIR + prefix + "-*" + "\"" + " | onessd "
        rid = submit_rule(rule_str)
        # Activate rule
        start_rule(rid)
        # Statue check
        rule = get_rule(rid)
        while rule['numCmdsGen'] != 3:
            rule = get_rule(rid)
        cids = get_cids_of_rule(rid)
        failed = wait_for_cmdlets(cids)
        self.assertTrue(len(failed) == 0)
        stop_rule(rid)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('unittest_args', nargs='*')
    args, unknown_args = parser.parse_known_args()
    sys.argv[1:] = unknown_args

    unittest.main()
