import unittest
from util import *


FILE_SIZE = 1 * 1024 * 1024


class TestMover_1MB_20W(unittest.TestCase):
    def test_20W_archive(self):
	#create 20W files and move to archive
        cmd_create_list = [] 
        cmd_move_list = []
	file_paths = []
	cids = []
	max_number = 200000
	#launch 20W cmdlet
	print "launch 20W create cmdlet"
        for num in range(max_number):
	    file_path, cid = create_random_file_parallel(FILE_SIZE)
	    file_paths.append(file_path)
	    cids.append(cid)
	print "move 20W file into archive"
	for num in range(max_number):
	    cids.append(move_cmdlet("archive",file_paths[num]))
	
	#check
	failed_cids = wait_for_cmdlets(cids)
	self.assertTrue(len(failed_cids) == 0)

    def test_20W_onessd(self):
        #create 20W files and move to archive
        cmd_create_list = []
        cmd_move_list = []
        file_paths = []
        cids = []
        max_number = 200000
        #launch 20W cmdlet
        print "launch 20W create cmdlet"
        for num in range(max_number):
            file_path, cid = create_random_file_parallel(FILE_SIZE)
            file_paths.append(file_path)
            cids.append(cid)
        print "move 20W file into archive"
        for num in range(max_number):
            cids.append(move_cmdlet("onessd",file_paths[num]))

        #check
        failed_cids = wait_for_cmdlets(cids)
        self.assertTrue(len(failed_cids) == 0)

    def test_20W_allssd(self):
        #create 20W files and move to archive
        cmd_create_list = []
        cmd_move_list = []
        file_paths = []
        cids = []
        max_number = 200000
        #launch 20W cmdlet
        print "launch 20W create cmdlet"
        for num in range(max_number):
            file_path, cid = create_random_file_parallel(FILE_SIZE)
            file_paths.append(file_path)
            cids.append(cid)
        print "move 20W file into archive"
        for num in range(max_number):
            cids.append(move_cmdlet("allssd",file_paths[num]))

        #check
        failed_cids = wait_for_cmdlets(cids)
        self.assertTrue(len(failed_cids) == 0)
	    



if __name__ == '__main__':
    unittest.main()
