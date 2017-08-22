import unittest
from util import *


class TestMoverProtection(unittest.TestCase):
    def test_mover_read(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(1024 * 1024 * 1024)
        # Begin to move file
        cid_move = submit_cmdlet("allssd -file " + file_path)
        check_storage(file_path)
        # read the file
        cid_read = read_file(file_path)
        # check the statement of read
        self.assertTrue(wait_for_cmdlet(cid_read)['state'] == "DONE")
        self.assertTrue(wait_for_cmdlet(cid_move)['state'] == "DONE")

    def test_mover_delete(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(1024 * 1024 * 1024)
        cid_move = submit_cmdlet("allssd -file " + file_path)
        check_storage(file_path)
        # delete the file
        cid_delete = delete_file(file_path)
        # check the statement of read
        self.assertTrue(wait_for_cmdlet(cid_delete)['state'] == "DONE")
        self.assertTrue(wait_for_cmdlet(cid_move)['state'] == "DONE")

    def test_mover_append(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(1024 * 1024 * 1024)
        cid_move = submit_cmdlet("allssd -file " + file_path)
        check_storage(file_path)
        # append random content to current file
        cid_append = append_file(file_path, random.randrange(1024, 1024 * 1024 * 2))
        # check the statement of read
        self.assertTrue(wait_for_cmdlet(cid_append)['state'] == "DONE")
        self.assertTrue(wait_for_cmdlet(cid_move)['state'] == "DONE")

    def test_mover_overwrite(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(1024 * 1024 * 1024)
        # Begin to move file
        cid_move = submit_cmdlet("allssd -file " + file_path)
        check_storage(file_path)
        # overwrite the file
        cid_delete = create_file(file_path, 24 * 1024 * 1024)
        # check the statement of read
        self.assertTrue(wait_for_cmdlet(cid_delete)['state'] == "DONE")
        self.assertTrue(wait_for_cmdlet(cid_move)['state'] == "DONE")


if __name__ == '__main__':
    unittest.main()
