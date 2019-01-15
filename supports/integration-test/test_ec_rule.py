import argparse
import unittest
from util import *
import datetime

class test_ec_rule(unittest.TestCase):

  def test_ec_rule(self):
    file_dir = TEST_DIR + random_string() + "/"
    print "The file path for test is {}.".format(file_dir)
    cids = []
    for i in range(MAX_NUMBER):
      path, cid = create_random_file_parallel(FILE_SIZE, file_dir)
      cids.append(cid)
    failed_cids = wait_for_cmdlets(cids)
    self.assertTrue(len(failed_cids) == 0, "Failed to create test files!")
    # wait for consistency
    time.sleep(10)
    # submit rule
    rule_str = "file : path matches \"" + file_dir + '*' + "\" | ec -policy " + POLICY
    rid = submit_rule(rule_str)
    # Activate rule
    start_rule(rid)

    start_time = datetime.datetime.now()

    # Status check
    rule = get_rule(rid)
    last_checked = rule['numChecked']
    last_cmdsgen = rule['numCmdsGen']

    time.sleep(.1)
    rule = get_rule(rid)
    while not ((rule['numChecked'] > last_checked) and (rule['numCmdsGen'] == last_cmdsgen)):
        time.sleep(.1)
        rule = get_rule(rid)
        last_checked = rule['numChecked']
        last_cmdsgen = rule['numCmdsGen']
        time.sleep(.1)
        rule = get_rule(rid)
    cids = get_cids_of_rule(rid)
    failed_cids = wait_for_cmdlets(cids)
    self.assertTrue(len(failed_cids) == 0, "Test failed for EC rule!")
    # wait for consistency

    end_time = datetime.datetime.now()
    interval = (end_time-start_time).seconds
    print 'ec_time:\t', interval

    time.sleep(10)
    stop_rule(rid)

  def test_unec_rule(self):
    file_dir_path = TEST_DIR + random_string()
    file_dir = file_dir_path + "/"
    print "The file path for test is {}.".format(file_dir)
    # create a test dir by creating a file under the dir firstly and then deleting it
    submit_cmdlet("write -file " + file_dir + "tmp.file -length 10")
    # wait the metastore sync
    time.sleep(10)
    # submit action
    action_str = "ec -file {} -policy {}".format(file_dir_path, POLICY)
    # Activate actions
    cid = submit_cmdlet(action_str)
    cmd = wait_for_cmdlet(cid)
    self.assertTrue(cmd['state'] == "DONE", "Failed to make the test dir an EC one.")
    cids = []
    for i in range(MAX_NUMBER):
      path, cid = create_random_file_parallel(FILE_SIZE, file_dir)
      cids.append(cid)
    failed_cids = wait_for_cmdlets(cids)
    self.assertTrue(len(failed_cids) == 0, "Failed to create test files!")
    # wait for consistency
    time.sleep(10)
    rule_str = "file : path matches \"" + file_dir + '*' + "\" | unec"
    rid = submit_rule(rule_str)
    # Activate rule
    start_rule(rid)

    start_time = datetime.datetime.now()

    # Status check
    rule = get_rule(rid)
    last_checked = rule['numChecked']
    last_cmdsgen = rule['numCmdsGen']

    time.sleep(.1)
    rule = get_rule(rid)
    while not ((rule['numChecked'] > last_checked) and (rule['numCmdsGen'] == last_cmdsgen)):
      time.sleep(.1)
      rule = get_rule(rid)
      last_checked = rule['numChecked']
      last_cmdsgen = rule['numCmdsGen']
      time.sleep(.1)
      rule = get_rule(rid)
    cids = get_cids_of_rule(rid)
    failed_cids = wait_for_cmdlets(cids)
    self.assertTrue(len(failed_cids) == 0, "Test failed for EC rule!")
    # wait for consistency

    end_time = datetime.datetime.now()
    interval = (end_time-start_time).seconds
    print 'unec_time:\t', interval

    time.sleep(10)
    stop_rule(rid)

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('-size', default='1MB',
                      help="size of file, Default Value 1MB.")
  parser.add_argument('-num', default='10',
                      help="file num, Default Value 10.")
  parser.add_argument('-policy', default='XOR-2-1-1024k',
                      help="EC policy, Default Value XOR-2-1-1024k.")
  parser.add_argument('unittest_args', nargs='*')
  args, unknown_args = parser.parse_known_args()
  sys.argv[1:] = unknown_args
  print "The file size for test is {}.".format(args.size)
  FILE_SIZE = convert_to_byte(args.size)
  print "The file number for test is {}.".format(args.num)
  MAX_NUMBER = int(args.num)
  print "The EC policy for test is {}.".format(args.policy)
  POLICY = args.policy

  unittest.main()