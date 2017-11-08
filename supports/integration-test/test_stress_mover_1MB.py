import unittest
from util import *

# 1MB
FILE_SIZE = 1024 * 1024


class TestStressMover_1MB(unittest.TestCase):
    def test_move_scheduler_1000(self):
        # 1K
        max_number = 1000
        file_paths = []
        cids = []
        failed_cids = []
        for i in range(max_number):
            file_paths.append(create_random_file(FILE_SIZE))
        for i in range(max_number):
            cids.append(move_randomly(file_paths[i]))
        while len(cids) != 0:
            cmd = wait_for_cmdlet(cids[0])
            if cmd['state'] == 'FAILED':
                failed_cids.append(cids[0])
            cids.pop(0)
        self.assertTrue(len(failed_cids) == 0)

    def test_move_scheduler_10000(self):
        # 10K
        max_number = 10000
        file_paths = []
        cids = []
        for i in range(max_number):
            file_paths.append(create_random_file(FILE_SIZE))
        for i in range(max_number):
            cids.append(move_randomly(file_paths[i]))
        failed_cids = wait_for_cmdlets(cids)
        self.assertTrue(len(failed_cids) == 0)

    def test_move_scheduler_50000(self):
        # 50K
        max_number = 50000
        file_paths = []
        cids = []
        for i in range(max_number):
            file_paths.append(create_random_file(FILE_SIZE))
        for i in range(max_number):
            cids.append(move_randomly(file_paths[i]))
        failed_cids = wait_for_cmdlets(cids)
        self.assertTrue(len(failed_cids) == 0)

    def test_move_scheduler_500000(self):
        # 500K
        max_number = 500000
        file_paths = []
        cids = []
        for i in range(max_number):
            file_paths.append(create_random_file(FILE_SIZE))
        for i in range(max_number):
            cids.append(move_randomly(file_paths[i]))
        failed_cids = wait_for_cmdlets(cids)
        self.assertTrue(len(failed_cids) == 0)


if __name__ == '__main__':
    unittest.main()
