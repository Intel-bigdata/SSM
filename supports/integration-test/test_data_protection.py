import argparse
import unittest
from util import *

move_actions = ['archive', 'alldisk', 'onedisk', 'allssd', 'onessd', 'cache', 'uncache']


class TestMoverProtection(unittest.TestCase):

    def test_mover_read(self):
        for action in move_actions:
            file_path = create_random_file(FILE_SIZE)
            # move
            cmd1 = wait_for_cmdlet(move_cmdlet(action, file_path))
            self.assertTrue(cmd1['state'] == "DONE", "Test failed for action {}".format(action))
            # read the file
            cmd2 = wait_for_cmdlet(read_file(file_path))
            self.assertTrue(cmd2['state'] == "DONE", "Test failed for action read after action {}".format(action))

    def test_mover_delete(self):
        for action in move_actions:
            file_path = create_random_file(FILE_SIZE)
            # move
            cmd1 = wait_for_cmdlet(move_cmdlet(action, file_path))
            self.assertTrue(cmd1['state'] == "DONE", "Test failed for action {}".format(action))
            # delete the file
            cmd2 = wait_for_cmdlet(delete_file(file_path))
            self.assertTrue(cmd2['state'] == "DONE", "Test failed for action delete after action {}".format(action))

    def test_mover_append(self):
        for action in move_actions:
            file_path = create_random_file(FILE_SIZE)
            # move
            cmd1 = wait_for_cmdlet(move_cmdlet(action, file_path))
            self.assertTrue(cmd1['state'] == "DONE", "Test failed for action {}".format(action))
            # append the file
            cmd2 = wait_for_cmdlet(append_file(file_path,
                                    random.randrange(1024, 1024 * 1024 * 2)))
            self.assertTrue(cmd2['state'] == "DONE", "Test failed for action append after action {}".format(action))

    def test_mover_overwrite(self):
        for action in move_actions:
            file_path = create_random_file(FILE_SIZE)
            # move
            cmd1 = wait_for_cmdlet(move_cmdlet(action, file_path))
            self.assertTrue(cmd1['state'] == "DONE", "Test failed for action {}".format(action))
            # overwrite the file
            cmd2 = wait_for_cmdlet(create_file(file_path, 24 * 1024 * 1024))
            self.assertTrue(cmd2['state'] == "DONE", "Test failed for action write after action {}".format(action))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-size', default='64MB')
    parser.add_argument('unittest_args', nargs='*')
    args, unknown_args = parser.parse_known_args()
    sys.argv[1:] = unknown_args
    print "The file size for test is {}.".format(args.size)
    FILE_SIZE = convert_to_byte(args.size)

    unittest.main()
