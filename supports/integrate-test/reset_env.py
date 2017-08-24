import unittest
from util import *


class ResetEnv(unittest.TestCase):

    # def test_delete_all_rules(self):
    #     """
    #     delete all rules
    #     """
    #     rules = list_rule()
    #     for rule in rules:
    #         # Delete all rules
    #         if rule['state'] != 'DELETED':
    #             delete_rule(rule['id'])
    #     rules = [r for rule in list_rule() if rule['state'] != 'DELETED']
    #     self.assertTrue(len(rules) == 0)

    # def test_delete_all_actions(self):
    #     """
    #     delete all actions, currently not supported
    #     """
    #     pass

    # def test_delete_all_files(self):
    #     subprocess.call("sh init_HDFS", shell=True)

    def test_create_10000_files(self):
        max_number = 10000
        file_paths = []
        cids = []
        for i in range(max_number):
            # 1 MB files
            file_path, cid = create_random_file_parallel(1024 * 1024)
            file_paths.append(file_path)
            cids.append(cid)
        wait_for_cmdlets(cids)


if __name__ == '__main__':
    unittest.main()
