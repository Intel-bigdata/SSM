import unittest
from util import *


class TestMoverProtection(unittest.TestCase):
    def test_mover_allssd_read(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(1024 * 1024 * 1024)
        # Begin to move file
        cid_moves = wait_for_cmdlet(move_cmdlet([MOVE_TYPE[1]], file_path))
        # read the file
        cid_read = read_file(file_path)
        # check the statement of read
        self.assertTrue(all_success(cid_moves))
        self.assertTrue(all_success([cid_read]))

    def test_mover_onessd_read(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(1024 * 1024 * 1024)
        # Begin to move file
        cid_moves = wait_for_cmdlet(move_cmdlet([MOVE_TYPE[0]], file_path))
        # read the file
        cid_read = read_file(file_path)
        # check the statement of read
        self.assertTrue(all_success(cid_moves))
        self.assertTrue(all_success([cid_read]))

    def test_mover_archive_read(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(1024 * 1024 * 1024)
        # Begin to move file
        cid_moves = wait_for_cmdlet(move_cmdlet([MOVE_TYPE[2]], file_path))
        # read the file
        cid_read = read_file(file_path)
        # check the statement of read
        self.assertTrue(all_success(cid_moves))
        self.assertTrue(all_success([cid_read]))

    def test_mover_allssd_delete(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(1024 * 1024 * 1024)
        cid_moves = wait_for_cmdlet(move_cmdlet([MOVE_TYPE[1]], file_path))
        # delete the file
        cid_delete = delete_file(file_path)
        # check the statement of read
        self.assertTrue(all_success(cid_moves))
        self.assertTrue(all_success([cid_delete]))

    def test_mover_onessd_delete(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(1024 * 1024 * 1024)
        cid_moves = wait_for_cmdlet(move_cmdlet([MOVE_TYPE[0]], file_path))
        # delete the file
        cid_delete = delete_file(file_path)
        # check the statement of read
        self.assertTrue(all_success(cid_moves))
        self.assertTrue(all_success([cid_delete]))

    def test_mover_archive_delete(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(1024 * 1024 * 1024)
        cid_moves = wait_for_cmdlet(move_cmdlet([MOVE_TYPE[2]], file_path))
        # delete the file
        cid_delete = delete_file(file_path)
        # check the statement of read
        self.assertTrue(all_success(cid_moves))
        self.assertTrue(all_success([cid_delete]))

    def test_mover_allssd_append(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(1024 * 1024 * 1024)
        cid_moves = wait_for_cmdlet(move_cmdlet([MOVE_TYPE[1]], file_path))
        # append random content to current file
        cid_append = append_file(file_path, random.randrange(1024, 1024 * 1024 * 2))
        # check the statement of read
        self.assertTrue(all_success(cid_moves))
        self.assertTrue(all_success([cid_append]))

    def test_mover_onessd_append(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(1024 * 1024 * 1024)
        cid_moves = wait_for_cmdlet(move_cmdlet([MOVE_TYPE[0]], file_path))
        # append random content to current file
        cid_append = append_file(file_path, random.randrange(1024, 1024 * 1024 * 2))
        # check the statement of read
        self.assertTrue(all_success(cid_moves))
        self.assertTrue(all_success([cid_append]))

    def test_mover_archive_append(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(1024 * 1024 * 1024)
        cid_moves = wait_for_cmdlet(move_cmdlet([MOVE_TYPE[2]], file_path))
        # append random content to current file
        cid_append = append_file(file_path, random.randrange(1024, 1024 * 1024 * 2))
        # check the statement of read
        self.assertTrue(all_success(cid_moves))
        self.assertTrue(all_success([cid_append]))

    def test_mover_allssd_overwrite(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(1024 * 1024 * 1024)
        cid_moves = wait_for_cmdlet(move_cmdlet([MOVE_TYPE[1]], file_path))
        check_storage(file_path)
        # overwrite the file
        cid_delete = create_file(file_path, 24 * 1024 * 1024)
        # check the statement of read
        self.assertTrue(all_success(cid_moves))
        self.assertTrue(all_success([cid_delete]))

    def test_mover_onessd_overwrite(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(1024 * 1024 * 1024)
        cid_moves = wait_for_cmdlet(move_cmdlet([MOVE_TYPE[0]], file_path))
        # overwrite the file
        cid_delete = create_file(file_path, 24 * 1024 * 1024)
        # check the statement of read
        self.assertTrue(all_success(cid_moves))
        self.assertTrue(all_success([cid_delete]))

    def test_mover_archive_overwrite(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(1024 * 1024 * 1024)
        cid_moves = wait_for_cmdlet(move_cmdlet([MOVE_TYPE[2]], file_path))
        # overwrite the file
        cid_delete = create_file(file_path, 24 * 1024 * 1024)
        # check the statement of read
        self.assertTrue(all_success(cid_moves))
        self.assertTrue(all_success([cid_delete]))

if __name__ == '__main__':
    unittest.main()
