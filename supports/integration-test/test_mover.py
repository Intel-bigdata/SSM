import argparse
import unittest
from util import *


class TestMover(unittest.TestCase):

    # test single move
    def test_single_move(self):
        for action in MOVE_TYPES:
            cmd_create, cmd_move = move_random_file(action, FILE_SIZE)
            self.assertTrue(cmd_create['state'] == "DONE")
            self.assertTrue(cmd_move['state'] == "DONE", "{} action failed".format(action))

    # test double moves for any pair of move actions
    def test_double_moves(self):
        for action1 in MOVE_TYPES:
            for action2 in MOVE_TYPES:
                    cmd_create, cmd_move_1, cmd_move_2 = move_random_file_twice(action1, action2, FILE_SIZE)
                    self.assertTrue(cmd_create['state'] == "DONE")
                    self.assertTrue(cmd_move_1['state'] == "DONE", "{} action failed".format(action1))
                    self.assertTrue(cmd_move_2['state'] == "DONE", "{} action failed".format(action2))

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
    parser = argparse.ArgumentParser()
    parser.add_argument('-size', default='64MB',
                        help="size of file, Default Value 64MB.")
    parser.add_argument('unittest_args', nargs='*')
    args, unknown_args = parser.parse_known_args()
    sys.argv[1:] = unknown_args
    print "The file size for test is {}.".format(args.size)
    FILE_SIZE = convert_to_byte(args.size)

    unittest.main()
