import unittest
from util import *


FILE_SIZE = 2 * 1024 * 1024 * 1024


class TestMoverProtection_2GB(unittest.TestCase):

    def test_mover_allssd_read(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(FILE_SIZE)
        # Begin to move file
        cmds = []
        cmds.append(move_cmdlet("allssd", file_path))
        # read the file
        cmds.append(read_file(file_path))
        failed = wait_for_cmdlets(cmds)
        # check the statement of read
        self.assertTrue(len(failed) == 0)

    def test_mover_onessd_read(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(FILE_SIZE)
        # Begin to move file
        cmds = []
        cmds.append(move_cmdlet("onessd", file_path))
        # read the file
        cmds.append(read_file(file_path))
        failed = wait_for_cmdlets(cmds)
        # check the statement of read
        self.assertTrue(len(failed) == 0)

    def test_mover_archive_read(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(FILE_SIZE)
        # Begin to move file
        cmds = []
        cmds.append(move_cmdlet("archive", file_path))
        # read the file
        cmds.append(read_file(file_path))
        failed = wait_for_cmdlets(cmds)
        # check the statement of read
        self.assertTrue(len(failed) == 0)

    def test_mover_allssd_delete(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(FILE_SIZE)
        cmds = []
        cmds.append(move_cmdlet("allssd", file_path))
        check_storage(file_path)
        # delete the file
        cmds.append(delete_file(file_path))
        failed = wait_for_cmdlets(cmds)
        # check the statement of read
        self.assertTrue(len(failed) == 0)

    def test_mover_onessd_delete(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(FILE_SIZE)
        cmds = []
        cmds.append(move_cmdlet("onessd", file_path))
        check_storage(file_path)
        # delete the file
        cmds.append(delete_file(file_path))
        failed = wait_for_cmdlets(cmds)
        # check the statement of read
        self.assertTrue(len(failed) == 0)

    def test_mover_archive_delete(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(FILE_SIZE)
        cmds = []
        cmds.append(move_cmdlet("archive", file_path))
        check_storage(file_path)
        # delete the file
        cmds.append(delete_file(file_path))
        failed = wait_for_cmdlets(cmds)
        # check the statement of read
        self.assertTrue(len(failed) == 0)

    def test_mover_allssd_append(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(FILE_SIZE)
        cmds = []
        cmds.append(move_cmdlet("allssd", file_path))
        # append random content to current file
        cmds.append(append_file(file_path,
                    random.randrange(1024, 1024 * 1024 * 2)))
        failed = wait_for_cmdlets(cmds)
        # check the statement of read
        self.assertTrue(len(failed) == 0)

    def test_mover_onessd_append(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(FILE_SIZE)
        cmds = []
        cmds.append(move_cmdlet("onessd", file_path))
        # append random content to current file
        cmds.append(append_file(file_path,
                    random.randrange(1024, 1024 * 1024 * 2)))
        failed = wait_for_cmdlets(cmds)
        # check the statement of read
        self.assertTrue(len(failed) == 0)

    def test_mover_archive_append(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(FILE_SIZE)
        cmds = []
        cmds.append(move_cmdlet("archive", file_path))
        # append random content to current file
        cmds.append(append_file(file_path,
                    random.randrange(1024, 1024 * 1024 * 2)))
        failed = wait_for_cmdlets(cmds)
        # check the statement of read
        self.assertTrue(len(failed) == 0)

    def test_mover_allssd_overwrite(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(FILE_SIZE)
        cmds = []
        cmds.append(move_cmdlet("allssd", file_path))
        # overwrite the file
        cmds.append(create_file(file_path, 24 * 1024 * 1024))
        failed = wait_for_cmdlets(cmds)
        # check the statement of read
        self.assertTrue(len(failed) == 0)

    def test_mover_onessd_overwrite(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(FILE_SIZE)
        cmds = []
        cmds.append(move_cmdlet("onessd", file_path))
        # overwrite the file
        cmds.append(create_file(file_path, 24 * 1024 * 1024))
        failed = wait_for_cmdlets(cmds)
        # check the statement of read
        self.assertTrue(len(failed) == 0)

    def test_mover_archive_overwrite(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 2 GB file
        file_path = create_random_file(FILE_SIZE)
        cmds = []
        cmds.append(move_cmdlet("archive", file_path))
        # overwrite the file
        cmds.append(create_file(file_path, 24 * 1024 * 1024))
        failed = wait_for_cmdlets(cmds)
        # check the statement of read
        self.assertTrue(len(failed) == 0)


if __name__ == '__main__':
    unittest.main()
