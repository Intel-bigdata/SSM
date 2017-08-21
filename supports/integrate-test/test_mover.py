import unittest
from util import *


class TestMover(unittest.TestCase):
    # move to archive
    def test_archive_10MB(self):
        cmd_create, cmd_move = move_random_file('archive', 10 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    def test_archive_64MB(self):
        cmd_create, cmd_move = move_random_file('archive', 64 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    def test_archive_1GB(self):
        cmd_create, cmd_move = move_random_file('archive', 1024 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    def test_archive_2GB(self):
        cmd_create, cmd_move = move_random_file('archive',
                                                2 * 1024 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    # move to onessd
    def test_onessd_10MB(self):
        cmd_create, cmd_move = move_random_file('onessd', 10 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    def test_onessd_64MB(self):
        cmd_create, cmd_move = move_random_file('onessd', 64 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    def test_onessd_1GB(self):
        cmd_create, cmd_move = move_random_file('onessd', 1024 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    def test_onessd_2GB(self):
        cmd_create, cmd_move = move_random_file('onessd',
                                                2 * 1024 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    # move to allssd
    def test_allssd_10MB(self):
        cmd_create, cmd_move = move_random_file('allssd', 10 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    def test_allssd_64MB(self):
        cmd_create, cmd_move = move_random_file('allssd', 64 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    def test_allssd_1GB(self):
        cmd_create, cmd_move = move_random_file('allssd', 1024 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    def test_allssd_2GB(self):
        cmd_create, cmd_move = move_random_file('allssd',
                                                2 * 1024 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    def test_archive_onessd_10MB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('archive', 'onessd', 10 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_archive_onessd_64MB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('archive', 'onessd', 64 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_archive_onessd_1GB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('archive', 'onessd', 1024 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_archive_onessd_2GB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('archive', 'onessd', 2 *1024 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_archive_allssd_10MB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('archive', 'allssd', 10 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_archive_allssd_64MB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('archive', 'allssd', 64 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_archive_allssd_1GB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('archive', 'allssd', 1024 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_archive_allssd_2GB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('archive', 'allssd', 2 * 1024 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_onessd_archive_10MB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('onessd', 'archive', 10 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_onessd_archive_64MB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('onessd', 'archive', 64 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_onessd_archive_1GB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('onessd', 'archive', 1024 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_onessd_archive_2GB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('onessd', 'archive', 2 * 1024 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_onessd_allssd_10MB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('onessd', 'allssd', 10 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_onessd_archive_64MB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('onessd', 'archive', 64 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_onessd_archive_1GB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('onessd', 'archive', 1024 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_onessd_archive_2GB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('onessd', 'archive', 2 * 1024 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_allssd_onessd_10MB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('allssd', 'onessd', 10 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_allssd_onessd_64MB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('allssd', 'onessd', 64 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_allssd_onessd_1GB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('allssd', 'onessd', 1024 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_allssd_onessd_2GB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('allssd', 'onessd', 2 * 1024 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_allssd_archive_10MB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('allssd', 'archive', 10 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_allssd_archive_64MB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('allssd', 'archive', 64 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_allssd_archive_1GB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('allssd', 'archive', 1014 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_allssd_archive_2GB(self):
        cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('allssd', 'archive', 2 * 1014 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move_1['state'] == "DONE")
        self.assertTrue(cmd_move_2['state'] == "DONE")

    def test_random_list_mover_10MB(self):
        # get the random test list
        cmds = move_random_task_list(10 * 1024 * 1024)
        # check the result
        self.assertTrue(all_success(cmds))

    def test_random_list_mover_64MB(self):
        # get the random test list
        cmds = move_random_task_list(10 * 1024 * 1024)
        # check the result
        # check the result
        self.assertTrue(all_success(cmds))

    def test_random_list_mover_1GB(self):
        # get the random test list
        cmds = move_random_task_list(1024 * 1024 * 1024)
        # check the result
        # check the result
        self.assertTrue(all_success(cmds))

    def test_random_list_mover_2GB(self):
        # get the random test list
        cmds = move_random_task_list(2 * 1024 * 1024 * 1024)
        # check the result
        # check the result
        self.assertTrue(all_success(cmds))

    def test_random_list_mover_totally_10MB(self):
        # get the random test list
        cmds = move_random_task_list_totally(10 * 1024 * 1024)
        # check the result
        # check the result
        self.assertTrue(all_success(cmds))

    def test_random_list_mover_totally_64MB(self):
        # get the random test list
        cmds = move_random_task_list_totally(10 * 1024 * 1024)
        # check the result
        # check the result
        self.assertTrue(all_success(cmds))

    def test_random_list_mover_totally_1GB(self):
        # get the random test list
        cmds = move_random_task_list_totally(1024 * 1024 * 1024)
        # check the result
        # check the result
        self.assertTrue(all_success(cmds))

    def test_random_list_mover_totally_2GB(self):
        # get the random test list
        cmds = move_random_task_list_totally(2 * 1024 * 1024 * 1024)
        # check the result
        # check the result
        self.assertTrue(all_success(cmds))


if __name__ == '__main__':
    unittest.main()
