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

    # def test_archive_1GB(self):
    #     cmd_create, cmd_move = move_random_file('archive', 1024 * 1024 * 1024)
    #     self.assertTrue(cmd_create['state'] == "DONE")
    #     self.assertTrue(cmd_move['state'] == "DONE")

    # def test_archive_2GB(self):
    #     cmd_create, cmd_move = move_random_file('archive',
    #                                            2 * 1024 * 1024 * 1024)
    #     self.assertTrue(cmd_create['state'] == "DONE")
    #     self.assertTrue(cmd_move['state'] == "DONE")

    # def test_archive_10GB(self):
    #     cmd_create, cmd_move = move_random_file('archive',
    #                                            10 * 1024 * 1024 * 1024)
    #     self.assertTrue(cmd_create['state'] == "DONE")
    #     self.assertTrue(cmd_move['state'] == "DONE")

    # move to onessd
    def test_onessd_10MB(self):
        cmd_create, cmd_move = move_random_file('onessd', 10 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    def test_onessd_64MB(self):
        cmd_create, cmd_move = move_random_file('onessd', 64 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    # def test_onessd_1GB(self):
    #     cmd_create, cmd_move = move_random_file('onessd', 1024 * 1024 * 1024)
    #     self.assertTrue(cmd_create['state'] == "DONE")
    #     self.assertTrue(cmd_move['state'] == "DONE")

    # def test_onessd_2GB(self):
    #     cmd_create, cmd_move = move_random_file('onessd',
    #                                            2 * 1024 * 1024 * 1024)
    #     self.assertTrue(cmd_create['state'] == "DONE")
    #     self.assertTrue(cmd_move['state'] == "DONE")

    # def test_onessd_10GB(self):
    #     cmd_create, cmd_move = move_random_file('onessd',
    #                                            10 * 1024 * 1024 * 1024)
    #     self.assertTrue(cmd_create['state'] == "DONE")
    #     self.assertTrue(cmd_move['state'] == "DONE")

    # move to allssd
    def test_allssd_10MB(self):
        cmd_create, cmd_move = move_random_file('allssd', 10 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    def test_allssd_64MB(self):
        cmd_create, cmd_move = move_random_file('allssd', 64 * 1024 * 1024)
        self.assertTrue(cmd_create['state'] == "DONE")
        self.assertTrue(cmd_move['state'] == "DONE")

    # def test_allssd_1GB(self):
    #     cmd_create, cmd_move = move_random_file('allssd', 1024 * 1024 * 1024)
    #     self.assertTrue(cmd_create['state'] == "DONE")
    #     self.assertTrue(cmd_move['state'] == "DONE")

    # def test_allssd_2GB(self):
    #     cmd_create, cmd_move = move_random_file('allssd',
    #                                            2 * 1024 * 1024 * 1024)
    #     self.assertTrue(cmd_create['state'] == "DONE")
    #     self.assertTrue(cmd_move['state'] == "DONE")

    # def test_allssd_10GB(self):
    #     cmd_create, cmd_move = move_random_file('allssd',
    #                                            10 * 1024 * 1024 * 1024)
    #     self.assertTrue(cmd_create['state'] == "DONE")
    #     self.assertTrue(cmd_move['state'] == "DONE")

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

    # def test_archive_onessd_1GB(self):
    #     cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('archive', 'onessd', 1024 * 1024 * 1024)
    #     self.assertTrue(cmd_create['state'] == "DONE")
    #     self.assertTrue(cmd_move_1['state'] == "DONE")
    #     self.assertTrue(cmd_move_2['state'] == "DONE")

    # def test_archive_onessd_2GB(self):
    #     cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('archive', 'onessd', 2 *1024 * 1024 * 1024)
    #     self.assertTrue(cmd_create['state'] == "DONE")
    #     self.assertTrue(cmd_move_1['state'] == "DONE")
    #     self.assertTrue(cmd_move_2['state'] == "DONE")

    # def test_archive_onessd_10GB(self):
    #     cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice('archive', 'onessd', 10 *1024 * 1024 * 1024)
    #     self.assertTrue(cmd_create['state'] == "DONE")
    #     self.assertTrue(cmd_move_1['state'] == "DONE")
    #     self.assertTrue(cmd_move_2['state'] == "DONE")


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

    def test_random_list_mover_10MB(self):
        file_path = "/test/test_file_10MB"
        cid = create_file(file_path,10*1024)
        check_storage(file_path)

        # use a list to save the result
        cid_list = []
        av_index = -1

        # get the random test list
        task_list = get_random_task_list(file_path,1000)

        while av_index < 1000:
            av_index = av_index + 1
            cid_list[av_index] = task_list[av_index]

        # check the result
        for i in range(1000):
            cmdlet = wait_for_cmdlet(cid_list[i])
            self.assertTrue(cmdlet['state'] == "DONE")

    def test_random_list_mover_64MB(self):
        file_path = "/test/test_file_10MB"
        cid = create_file(file_path,64*1024)
        check_storage(file_path)

        # use a list to save the result
        cid_list = []
        av_index = -1

        # get the random test list
        task_list = get_random_task_list(file_path,1000)

        while av_index < 1000:
            av_index = av_index + 1
            cid_list[av_index] = task_list[av_index]

        # check the result
        for i in range(1000):
            cmdlet = wait_for_cmdlet(cid_list[i])
            self.assertTrue(cmdlet['state'] == "DONE")


if __name__ == '__main__':
    unittest.main()
