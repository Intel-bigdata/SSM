import unittest
from util import *


class TestPerformance(unittest.TestCase):
    def test_mover_sequentially(self):
        # Case 1
        # random move sequentially
        queue = []
        count = len(MOVE_TYPE)
        while count > 0:
            # Submit cmdlets
            for index in range(len(TEST_FILES)):
                cmdlet = wait_for_cmdlet(random_move_test_file(TEST_FILES[index]))
                print cmdlet
                wait_for_cmdlet(check_storage(TEST_FILES[index]))
                if cmdlet is None or cmdlet['state'] == "FAILED":
                    queue.append(cmdlet['cid'])
            count -= 1
        print "Failed cids = {}".format(queue)
        self.assertTrue(len(queue) == 0)

    def test_mover_parallel(self):
        # Case 2
        # Parallel random move
        queue = []
        count = len(MOVE_TYPE)
        while count > 0:
            # Submit cmdlets
            for index in range(len(TEST_FILES)):
                queue.append(random_move_test_file(TEST_FILES[index]))
            # check status
            while len(queue) != 0:
                print wait_for_cmdlet(queue[0])
                queue.pop(0)
            count -= 1
        print "Failed cids = {}".format(queue)
        self.assertTrue(len(queue) == 0)

    def test_200_move_tasks(self):
        max_number = 200
        file_paths = []
        cids = []
        for i in range(max_number):
            # 150 MB files
            file_paths.append(create_random_file(150 * 1024 * 1024))
        for i in range(max_number):
            cids.append(move_randomly(file_paths[i]))
        while len(cids) != 0:
            wait_for_cmdlet(cids[0])
            cids.pop(0)
        self.assertTrue(len(cids) == 0)

    def test_500_move_tasks(self):
        max_number = 500
        file_paths = []
        cids = []
        for i in range(max_number):
            # 150 MB files
            file_paths.append(create_random_file(150 * 1024 * 1024))
        for i in range(max_number):
            cids.append(move_randomly(file_paths[i]))
        while len(cids) != 0:
            wait_for_cmdlet(cids[0])
            cids.pop(0)
        self.assertTrue(len(cids) == 0)

    def test_mover_stress(self):
        # TODO launch 100000 move actions
        pass


if __name__ == '__main__':
    unittest.main()
