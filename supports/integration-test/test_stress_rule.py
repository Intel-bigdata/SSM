import argparse
import unittest
from util import *

# In smart-default, smart.rule.executors confines the parallelism for SSM rules.
# To make stress test for rules, this property's value should be set large enough.


class TestStressRule(unittest.TestCase):

    def test_rule(self):
        rids = []
        for i in range(MAX_NUMBER):
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
    parser = argparse.ArgumentParser()
    parser.add_argument('-num', default='100',
                        help="file num, Default Value 100.")
    parser.add_argument('unittest_args', nargs='*')
    args, unknown_args = parser.parse_known_args()
    sys.argv[1:] = unknown_args
    print "A reminder: the value for smart.rule.executors in smart-default.xml" \
          " should be set large enough."
    print "The rule number for test is {}.".format(args.num)
    MAX_NUMBER = int(args.num)

    unittest.main()
