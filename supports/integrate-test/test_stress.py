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

    def test_mover_read(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 10 GB file
        file_path = TEST_FILES[0]
        cid_move = submit_cmdlet("allssd -file " + file_path)
        check_storage(file_path)
        # read the file
        cid_read = read_file(file_path)
        # check the statement of read
        self.assertTrue(wait_for_cmdlet(cid=cid_read)['state'] == "DONE")
        self.assertTrue(wait_for_cmdlet(cid=cid_move)['state'] == "DONE")

    def test_mover_write(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 10 GB file
        file_path = TEST_FILES[0]
        cid_move = submit_cmdlet("allssd -file " + file_path)
        check_storage(file_path)
        # read the file
        cid_read = read_file(file_path)
        # check the statement of read
        self.assertTrue(wait_for_cmdlet(cid_read)['state'] == "DONE")
        self.assertTrue(wait_for_cmdlet(cid_move)['state'] == "DONE")

        file_path = TEST_FILES[1]
        cid_move = submit_cmdlet("onessd -file " + file_path)
        check_storage(file_path)
        # read the file
        cid_read = read_file(file_path)
        # check the statement of read
        self.assertTrue(wait_for_cmdlet(cid_read)['state'] == "DONE")
        self.assertTrue(wait_for_cmdlet(cid_move)['state'] == "DONE")

    def test_mover_stress(self):
        # TODO launch 100000 move actions
        pass


if __name__ == '__main__':
    unittest.main()
