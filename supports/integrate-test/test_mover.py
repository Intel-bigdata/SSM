import unittest
from util import *

# def mover_from_archive_10MB(dest_type):
#     randomNum = random.randInt(1, 99999999)
#     file_path_10MB = "/test/" + randomNum + "_10MB"
#     cmd_create_10MB = create_file(file_path_10MB, 10 * 1024)
#     cid_dest_10MB = submit_cmdlet(dest_type + " -file " + file_path_10MB)
#     self.assertTrue(wait_for_cmdlet(cmd_create_10MB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_dest_10MB)['state'] == "DONE")
#     file_path_64MB = "/test/" + randomNum + "_64MB"
#     cmd_create_64MB = create_file(file_path_64MB, 64 * 1024)
#     cid_dest_64MB = submit_cmdlet(dest_type + " -file " + file_path_64MB)
#     self.assertTrue(wait_for_cmdlet(cmd_create_64MB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_dest_64MB)['state'] == "DONE")

#     file_path_1GB = "/test/" + randomNum + "_1GB"
#     cmd_create_1GB = create_file(file_path_1GB, 1024 * 1024)
#     cid_dest_1GB = submit_cmdlet(dest_type + " -file " + file_path_1GB)
#     self.assertTrue(wait_for_cmdlet(cmd_create_1GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_dest_1GB)['state'] == "DONE")

#     file_path_2GB = "/test/" + randomNum + "_2GB"
#     cmd_create_2GB = create_file(file_path_2GB, 2 * 1024 * 1024)
#     cid_dest_2GB = submit_cmdlet(dest_type + " -file " + file_path_2GB)
#     self.assertTrue(wait_for_cmdlet(cmd_create_2GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_dest_2GB)['state'] == "DONE")

#     file_path_10GB = "/test/" + randomNum + "10GB"
#     cmd_create_10GB = create_file(file_path_10GB, 10 * 1024 * 1024)
#     cid_dest_10GB = submit_cmdlet(dest_type + " -file " + file_path_10GB)
#     self.assertTrue(wait_for_cmdlet(cmd_create_10GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_dest_10GB)['state'] == "DONE")


# def test_mover_from_archive_init():
#     mover_from_archive("archive")
#     mover_from_archive("onessd")
#     mover_from_archive("allssd")


# def mover_from_onessd(dest_type):
#     randomNum = random.randInt(1, 99999999)

#     file_path_10MB = "/test/" + randomNum + "_10MB"
#     file_path_64MB = "/test/" + randomNum + "_64MB"
#     file_path_1GB = "/test/" + randomNum + "_1GB"
#     file_path_2GB = "/test/" + randomNum + "_2GB"
#     file_path_10GB = "/test/" + randomNum + "10GB"

#     cmd_create_10MB = create_file(file_path_10MB, 10 * 1024)
#     cid_onessd_10MB = submit_cmdlet("onessd -file " + file_path_10MB)
#     cid_dest_10MB = submit_cmdlet(dest_type + " -file " + file_path_10MB)
#     self.assertTrue(wait_for_cmdlet(cid_onessd_10MB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_10MB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_dest_10MB)['state'] == "DONE")

#     cmd_create_64MB = create_file(file_path_64MB, 64 * 1024)
#     cid_onessd_64MB = submit_cmdlet("onessd -file " + file_path_64MB)
#     cid_dest_64MB = submit_cmdlet(dest_type + " -file " + file_path_64MB)
#     self.assertTrue(wait_for_cmdlet(cid_onessd_64MB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_64MB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_dest_64MB)['state'] == "DONE")

#     cmd_create_1GB = create_file(file_path_1GB, 1024 * 1024)
#     cid_onessd_1GB = submit_cmdlet("onessd -file " + file_path_1GB)
#     cid_dest_1GB = submit_cmdlet(dest_type + " -file " + file_path_1GB)
#     self.assertTrue(wait_for_cmdlet(cid_onessd_1GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_1GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_dest_1GB)['state'] == "DONE")

#     cmd_create_2GB = create_file(file_path_2GB, 2 * 1024 * 1024)
#     cid_onessd_2GB = submit_cmdlet("onessd -file " + file_path_2GB)
#     cid_dest_2GB = submit_cmdlet(dest_type + " -file " + file_path_2GB)
#     self.assertTrue(wait_for_cmdlet(cid_onessd_2GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_2GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_dest_2GB)['state'] == "DONE")

#     cmd_create_10GB = create_file(file_path_10GB, 10 * 1024 * 1024)
#     cid_onessd_10GB = submit_cmdlet("onessd -file " + file_path_10GB)
#     cid_dest_10GB = submit_cmdlet(dest_type + " -file " + file_path_10GB)
#     self.assertTrue(wait_for_cmdlet(cid_onessd_10GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_10GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_dest_10GB)['state'] == "DONE")

# def test_mover_from_onessd_init():
#     mover_from_onessd("archive")
#     mover_from_onessd("allssd")
#     mover_from_onessd("onessd")

# def mover_from_allssd(dest_type):
#     randomNum = random.randInt(1, 99999999)

#     file_path_10MB = "/test/" + randomNum + "_10MB"
#     file_path_64MB = "/test/" + randomNum + "_64MB"
#     file_path_1GB = "/test/" + randomNum + "_1GB"
#     file_path_2GB = "/test/" + randomNum + "_2GB"
#     file_path_10GB = "/test/" + randomNum + "10GB"

#     cmd_create_10MB = create_file(file_path_10MB, 10 * 1024)
#     cid_allssd_10MB = submit_cmdlet("allssd -file " + file_path_10MB)
#     cid_dest_10MB = submit_cmdlet(dest_type + " -file " + file_path_10MB)
#     self.assertTrue(wait_for_cmdlet(cid_allssd_10MB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_10MB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_dest_10MB)['state'] == "DONE")

#     cmd_create_64MB = create_file(file_path_64MB, 64 * 1024)
#     cid_allssd_64MB = submit_cmdlet("allssd -file " + file_path_64MB)
#     cid_dest_64MB = submit_cmdlet(dest_type + " -file " + file_path_64MB)
#     self.assertTrue(wait_for_cmdlet(cid_allssd_64MB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_64MB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_dest_64MB)['state'] == "DONE")

#     cmd_create_1GB = create_file(file_path_1GB, 1024 * 1024)
#     cid_allssd_1GB = submit_cmdlet("allssd -file " + file_path_1GB)
#     cid_dest_1GB = submit_cmdlet(dest_type + " -file " + file_path_1GB)
#     self.assertTrue(wait_for_cmdlet(cid_allssd_1GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_1GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_dest_1GB)['state'] == "DONE")

#     cmd_create_2GB = create_file(file_path_2GB, 2 * 1024 * 1024)
#     cid_allssd_2GB = submit_cmdlet("allssd -file " + file_path_2GB)
#     cid_dest_2GB = submit_cmdlet(dest_type + " -file " + file_path_2GB)
#     self.assertTrue(wait_for_cmdlet(cid_allssd_2GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_2GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_dest_2GB)['state'] == "DONE")

#     cmd_create_10GB = create_file(file_path_10GB, 10 * 1024 * 1024)
#     cid_allssd_10GB = submit_cmdlet("allssd -file " + file_path_10GB)
#     cid_dest_10GB = submit_cmdlet(dest_type + " -file " + file_path_10GB)
#     self.assertTrue(wait_for_cmdlet(cid_allssd_10GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_10GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_dest_10GB)['state'] == "DONE")

# def test_mover_from_allssd_init():
#     mover_from_allssd("archive")
#     mover_from_allssd("allssd")
#     mover_from_allssd("onessd")

# #the case doesn't include overwrite
# def mover_while_doing_other_operation(movetype,otheroperation):
#     randomNum = random.randInt(1, 99999999)

#     file_path_10MB = "/test/" + randomNum + "_10MB"
#     file_path_64MB = "/test/" + randomNum + "_64MB"
#     file_path_1GB = "/test/" + randomNum + "_1GB"
#     file_path_2GB = "/test/" + randomNum + "_2GB"
#     file_path_10GB = "/test/" + randomNum + "10GB"

#     cmd_create_10MB = create_file(file_path_10MB, 10 * 1024)
#     cid_movetype_10MB = submit_cmdlet(movetype + " -file " + file_path_10MB)
#     cid_otheroperation_10MB = submit_cmdlet(otheroperation + " -file " + file_path_10MB)
#     self.assertTrue(wait_for_cmdlet(cid_otheroperation_10MB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_10MB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_movetype_10MB)['state'] == "DONE")

#     cmd_create_64MB = create_file(file_path_64MB, 64 * 1024)
#     cid_movetype_64MB = submit_cmdlet(movetype_type + " -file " + file_path_64MB)
#     cid_otheroperation_64MB = submit_cmdlet(otheroperation + " -file " + file_path_64MB)
#     self.assertTrue(wait_for_cmdlet(cid_otheroperation_64MB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_64MB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_movetype_64MB)['state'] == "DONE")

#     cmd_create_1GB = create_file(file_path_1GB, 1024 * 1024)
#     cid_movetype_1GB = submit_cmdlet(movetype_type + " -file " + file_path_1GB)
#     cid_otheroperation_1GB = submit_cmdlet(otheroperation + " -file " + file_path_1GB)
#     self.assertTrue(wait_for_cmdlet(cid_otheroperation_1GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_1GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_movetype_1GB)['state'] == "DONE")

#     cmd_create_2GB = create_file(file_path_2GB, 2 * 1024 * 1024)
#     cid_movetype_2GB = submit_cmdlet(movetype_type + " -file " + file_path_2GB)
#     cid_otheroperation_2GB = submit_cmdlet(otheroperation + " -file " + file_path_2GB)
#     self.assertTrue(wait_for_cmdlet(cid_otheroperation_2GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_2GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_movetype_2GB)['state'] == "DONE")

#     cmd_create_10GB = create_file(file_path_10GB, 10 * 1024 * 1024)
#     cid_movetype_10GB = submit_cmdlet(movetype_type + " -file " + file_path_10GB)
#     cid_otheroperation_10GB = submit_cmdlet(otheroperation_type + " -file " + file_path_10GB)
#     self.assertTrue(wait_for_cmdlet(cid_otheroperation_10GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_10GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_movetype_10GB)['state'] == "DONE")

# def test_mover_while_doing_other_operation_init():
#     mover_while_doing_other_operation("allssd","read")
#     mover_while_doing_other_operation("allssd","delete")
#     mover_while_doing_other_operation("allssd", "append")

#     mover_while_doing_other_operation("onessd", "read")
#     mover_while_doing_other_operation("onessd", "delete")
#     mover_while_doing_other_operation("onessd", "append")

#     mover_while_doing_other_operation("archive", "read")
#     mover_while_doing_other_operation("archive", "delete")
#     mover_while_doing_other_operation("archive", "append")

# def mover_while_doing_overwrite(movetype):
#     randomNum = random.randInt(1, 99999999)

#     file_path_10MB = "/test/" + randomNum + "_10MB"
#     file_path_64MB = "/test/" + randomNum + "_64MB"
#     file_path_1GB = "/test/" + randomNum + "_1GB"
#     file_path_2GB = "/test/" + randomNum + "_2GB"
#     file_path_10GB = "/test/" + randomNum + "10GB"

#     cmd_create_10MB = create_file(file_path_10MB, 10 * 1024)
#     cid_movetype_10MB = submit_cmdlet(movetype + " -file " + file_path_10MB)
#     cid_write_10MB = create_file(file_path_10MB, 10 * 1024)
#     self.assertTrue(wait_for_cmdlet(cid_write_10MB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_10MB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_movetype_10MB)['state'] == "DONE")

#     cmd_create_64MB = create_file(file_path_64MB, 64 * 1024)
#     cid_movetype_64MB = submit_cmdlet(movetype_type + " -file " + file_path_64MB)
#     cid_write_64MB = create_file(file_path_64MB, 64 * 1024)
#     self.assertTrue(wait_for_cmdlet(cid_write_64MB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_64MB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_movetype_64MB)['state'] == "DONE")

#     cmd_create_1GB = create_file(file_path_1GB, 1024 * 1024)
#     cid_movetype_1GB = submit_cmdlet(movetype_type + " -file " + file_path_1GB)
#     cid_write_1GB = create_file(file_path_1GB, 1024 * 1024)
#     self.assertTrue(wait_for_cmdlet(cid_write_1GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_1GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_movetype_1GB)['state'] == "DONE")

#     cmd_create_2GB = create_file(file_path_2GB, 2 * 1024 * 1024)
#     cid_movetype_2GB = submit_cmdlet(movetype_type + " -file " + file_path_2GB)
#     cid_write_2GB = create_file(file_path_2GB, 2 * 1024 * 1024)
#     self.assertTrue(wait_for_cmdlet(cid_write_2GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_2GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_movetype_2GB)['state'] == "DONE")

#     cmd_create_10GB = create_file(file_path_10GB, 10 * 1024 * 1024)
#     cid_movetype_10GB = submit_cmdlet(movetype_type + " -file " + file_path_10GB)
#     cid_write_10GB = create_file(file_path_10GB, 10 * 1024 * 1024)
#     self.assertTrue(wait_for_cmdlet(cid_write_10GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cmd_create_10GB)['state'] == "DONE")
#     self.assertTrue(wait_for_cmdlet(cid_movetype_10GB)['state'] == "DONE")

# def test_mover_while_doing_overwrite_init():
#     mover_while_doing_overwrite("allssd")
#     mover_while_doing_overwrite("onessd")
#     mover_while_doing_overwrite("archive")


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


if __name__ == '__main__':
    unittest.main()
