import unittest
import time
from util import *


class TestStressRule(unittest.TestCase):

    def test_rule_200(self):
        max_number = 200
        rids = []
        for i in range(max_number):
            rule_str = "file: " + \
                "every 4s from now to now + 1d |" + \
                " path matches " + \
                "\"/ssmtest/" + random_string()[:5] + " *\"" + \
                " | onessd "
            rids.append(submit_rule(rule_str))
        # activate all rules
        for rid in rids:
            start_rule(rid)
        # sleep 60s
        time.sleep(60)
        for rid in rids:
            delete_rule(rid)

    def test_rule_500(self):
        max_number = 500
        rids = []
        for i in range(max_number):
            rule_str = "file: " + \
                "every 4s from now to now + 1d |" + \
                " path matches " + \
                "\"/ssmtest/" + random_string()[:5] + " *\"" + \
                " | onessd "
            rids.append(submit_rule(rule_str))
        # activate all rules
        for rid in rids:
            start_rule(rid)
        # sleep 60s
        time.sleep(60)
        for rid in rids:
            delete_rule(rid)

    def test_rule_1000(self):
        max_number = 1000
        rids = []
        for i in range(max_number):
            rule_str = "file: " + \
                "every 4s from now to now + 1d |" + \
                " path matches " + \
                "\"/ssmtest/" + random_string()[:5] + " *\"" + \
                " | onessd "
            rids.append(submit_rule(rule_str))
        # activate all rules
        for rid in rids:
            start_rule(rid)
        # sleep 60s
        time.sleep(60)
        for rid in rids:
            delete_rule(rid)

    def test_rule_10000(self):
        max_number = 10000
        rids = []
        for i in range(max_number):
            rule_str = "file: " + \
                "every 4s from now to now + 1d |" + \
                " path matches " + \
                "\"/ssmtest/" + random_string()[:5] + " *\"" + \
                " | onessd "
            rids.append(submit_rule(rule_str))
        # activate all rules
        for rid in rids:
            start_rule(rid)
        # sleep 60s
        time.sleep(60)
        for rid in rids:
            delete_rule(rid)


if __name__ == '__main__':
    unittest.main()
