import argparse
import unittest
from util import *

POLICY = 'XOR-2-1-1024k'
class test_ec_action(unittest.TestCase):

  def test_ec_rule(self):
    file_dir = TEST_DIR + random_string() + "/"
    print "The file path for test is {}.".format(file_dir)
    for i in range(MAX_NUMBER):
      create_file(file_dir + random_string(), FILE_SIZE)
    # submit rule
    rule_str = "file : path matches \"" + file_dir + '*' + "\" | ec -policy " + POLICY
    rid = submit_rule(rule_str)
    # Activate rule
    start_rule(rid)
    # Status check
    rule = get_rule(rid)
    cmds = get_cmdlets_of_rule(rid)
    flag = True
    for cmd in cmds:
      if cmd['state'] != 'DONE':
        flag = False
        break
    self.assertTrue(flag, "Test failed for ec rule.")
    stop_rule(rid)

  def test_unec_rule(self):
    file_dir_path = TEST_DIR + random_string()
    file_dir = file_dir_path + "/"
    print "The file path for test is {}.".format(file_dir)
    # submit action
    action_str = "ec -file {} -policy {}".format(file_dir_path, POLICY)
    # Activate actions
    cid = submit_cmdlet(action_str)
    cmd = wait_for_cmdlet(cid)
    for i in range(MAX_NUMBER):
      create_file(file_dir + random_string(), FILE_SIZE)

    rule_str = "file : path matches \"" + file_dir + '*' + "\" | unec"
    rid = submit_rule(rule_str)
    # Activate rule
    start_rule(rid)
    # Status check
    cmds = get_cmdlets_of_rule(rid)
    flag = True
    for cmd in cmds:
      if cmd['state'] != 'DONE':
        flag = False
        break
    self.assertTrue(flag, "Test failed for unec rule.")
    stop_rule(rid)

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('-size', default='1MB')
  parser.add_argument('-num', default='10')
  parser.add_argument('unittest_args', nargs='*')
  args, unknown_args = parser.parse_known_args()
  sys.argv[1:] = unknown_args
  print "The file size for test is {}.".format(args.size)
  FILE_SIZE = convert_to_byte(args.size)
  print "The file number for test is {}.".format(args.num)
  MAX_NUMBER = int(args.num)

  unittest.main()