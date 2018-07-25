import unittest
from util import *


class TestPerformance(unittest.TestCase):
    def test_cmdlet_scheduler_1000(self):
        max_number = 1000
        file_paths = []
        cids = []
        failed_cids = []
        for i in range(max_number):
            # 1 MB files
            file_path, cid = create_random_file_parallel(10 * 1024 * 1024)
            file_paths.append(file_path)
            cids.append(cid)
        for i in range(max_number):
            cids.append(read_file(file_paths[i]))
        for i in range(max_number):
            cids.append(delete_file(file_paths[i]))
        failed_cids = wait_for_cmdlets(cids)
        self.assertTrue(len(failed_cids) == 0)

    def test_cmdlet_scheduler_10000(self):
        max_number = 10000
        file_paths = []
        cids = []
        failed_cids = []
        for i in range(max_number):
            # 1 MB files
            file_path, cid = create_random_file_parallel(1024 * 1024)
            file_paths.append(file_path)
            cids.append(cid)
        for i in range(max_number):
            cids.append(read_file(file_paths[i]))
        for i in range(max_number):
            cids.append(delete_file(file_paths[i]))
        failed_cids = wait_for_cmdlets(cids)
        self.assertTrue(len(failed_cids) == 0)


if __name__ == '__main__':
    unittest.main()
