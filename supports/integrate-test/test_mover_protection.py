import unittest
from util import *


class TestMoverProtection(unittest.TestCase):
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

    def test_mover_delete(self):
        pass

    def test_mover_append(self):
        pass

    def test_mover_overwrite(self):
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


if __name__ == '__main__':
    unittest.main()
