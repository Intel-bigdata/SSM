import unittest
from util import *


class ResetEnv(unittest.TestCase):

    def test_delete_all_rules(self):
        """
        delete all rules
        """
        rules = list_rule()
        for rule in rules:
            # Delete all rules
            if rule['state'] != 'DELETED':
                delete_rule(rule['id'])
        rules = [r for rule in list_rule() if rule['state'] != 'DELETED']
        self.assertTrue(len(rules) == 0)

    def test_delete_all_actions(self):
        """
        delete all actions, currently not supported
        """
        pass

    def test_delete_all_files(self):
        subprocess.call("sh init_HDFS", shell=True)


if __name__ == '__main__':
    unittest.main()
