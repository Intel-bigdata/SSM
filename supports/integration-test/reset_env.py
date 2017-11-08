import unittest
from util import *

FILE_SIZE = 1024 * 1024


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

    # def test_delete_all_actions(self):
    #     """
    #     delete all actions, currently not supported
    #     """
    #     pass

    def test_delete_all_files(self):
        try:
            subprocess.call("hdfs dfs -rm " + TEST_DIR + "*", shell=True)
        except OSError:
            print "HDFS Envs is not configured!"

    def test_create_10000_1MB(self):
        """
        Create 10000 * 1 MB files in /1MB/
        """
        max_number = 10000
        file_paths = []
        cids = []
        for i in range(max_number):
            # 1 MB files
            file_path, cid = create_random_file_parallel("/1MB/", FILE_SIZE)
            file_paths.append(file_path)
            cids.append(cid)
        wait_for_cmdlets(cids)

    def test_create_10000_10MB(self):
        """
        Create 10000 * 10 MB files in /10MB/
        """
        max_number = 10000
        file_paths = []
        cids = []
        for i in range(max_number):
            # 1 MB files
            file_path, cid = create_random_file_parallel("/10MB/", FILE_SIZE)
            file_paths.append(file_path)
            cids.append(cid)
        wait_for_cmdlets(cids)

    def test_create_1000_100MB(self):
        """
        Create 1000 * 100 MB files in /100MB/
        """
        max_number = 1000
        file_paths = []
        cids = []
        for i in range(max_number):
            # 1 MB files
            file_path, cid = create_random_file_parallel("/100MB/", FILE_SIZE)
            file_paths.append(file_path)
            cids.append(cid)
        wait_for_cmdlets(cids)

    def test_create_files_10000(self):
        """
        Create 10000 * 1 MB files in /test/
        """
        max_number = 10000
        file_paths = []
        cids = []
        for i in range(max_number):
            # 1 MB files
            file_path, cid = create_random_file_parallel(FILE_SIZE)
            file_paths.append(file_path)
            cids.append(cid)
        wait_for_cmdlets(cids)

    def test_create_files_1000(self):
        """
        Create 1000 * 1 MB files  in /test/
        """
        max_number = 10000
        file_paths = []
        cids = []
        for i in range(max_number):
            # 1 MB files
            file_path, cid = create_random_file_parallel(FILE_SIZE)
            file_paths.append(file_path)
            cids.append(cid)
        wait_for_cmdlets(cids)

    def test_create_files_100(self):
        """
        Create 100 * 1 MB files  in /test/
        """
        max_number = 10000
        file_paths = []
        cids = []
        for i in range(max_number):
            # 1 MB files
            file_path, cid = create_random_file_parallel(FILE_SIZE)
            file_paths.append(file_path)
            cids.append(cid)
        wait_for_cmdlets(cids)


if __name__ == '__main__':
    unittest.main()
