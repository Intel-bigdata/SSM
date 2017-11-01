import unittest
from util import *


FILE_SIZE = 1 * 1024 * 1024

BACK_UP_SERVER_IP = "hdfs://namenode_ip"
BACK_UP_SERVER_PORT = "9000"



class TestCopy_1MB_5W(unittest.TestCase):
    def test_5W_copy(self):
	#add rule
	rule_str = "file : path matches \"/test/1MB/*\" | sync -dest "+ \
			   BACK_UP_SERVER_IP + ":" + BACK_UP_SERVER_PORT + "/test "
	rid = submit_rule(rule_str)
	# Activate rule
	start_rule(rid)

	#create 20W files and move to archive
	file_paths = []
	cids = []
	max_number = 50000
	#launch 5W cmdlet
	print "launch 5W create cmdlet"
        for num in range(max_number):
	    file_path, cid = create_random_file_parallel("/test/1MB/",FILE_SIZE)
	    file_paths.append(file_path)
	    cids.append(cid)
	#check
	failed_cids = wait_for_cmdlets(cids)
	print len(failed_cids)
	self.assertTrue(len(failed_cids) == 0)
	


if __name__ == '__main__':
    requests.adapters.DEFAULT_RETRIES = 5
    s = requests.session()
    s.keep_alive = False
    unittest.main()
