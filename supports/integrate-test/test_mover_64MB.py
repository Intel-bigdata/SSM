import unittest
from util import *


class TestMover_64MB(unittest.TestCase):
    FILE_SIZE = 64 * 1024 * 1024

    # move to archive
    def test_archive(self):
        cmd_create, cmd_move = move_random_file('archive', FILE_SIZE)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    # move to onessd
    def test_onessd(self):
        cmd_create, cmd_move = move_random_file('onessd', FILE_SIZE)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    # move to allssd
    def test_allssd(self):
        cmd_create, cmd_move = move_random_file('allssd', FILE_SIZE)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    # move to archive then onessd
    def test_archive_onessd(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('archive', 'onessd', FILE_SIZE)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    # move to archive then allssd
    def test_archive_allssd(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('archive', 'allssd', FILE_SIZE)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    # move to onessd then archive
    def test_onessd_archive(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('onessd', 'archive', FILE_SIZE)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    # move to onessd then allssd
    def test_onessd_allssd(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('onessd', 'allssd', FILE_SIZE)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    # move to allssd then onessd
    def test_allssd_onessd(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('allssd', 'onessd', FILE_SIZE)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    # move to allssd then archive
    def test_allssd_archive(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('allssd', 'archive', FILE_SIZE)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    # move randomly and continually
    def test_random_list_mover(self):
        # get the random test list
        cmds = move_random_task_list(FILE_SIZE)
        # check the result
        self.assertTrue(all_success(cmds))

    # move randomly and continually, nearby mover type can be the same
    def test_random_list_mover_totally(self):
        # get the random test list
        cmds = move_random_task_list_totally(FILE_SIZE)
        # check the result
        # check the result
        self.assertTrue(all_success(cmds))


if __name__ == '__main__':
    unittest.main()
