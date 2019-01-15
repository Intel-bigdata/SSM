import argparse
import unittest
from util import *

move_actions = ['archive', 'alldisk', 'onedisk', 'allssd', 'onessd', 'cache', 'uncache']


class TestMoverProtection(unittest.TestCase):

    def test_mover_read(self):
        for action in move_actions:
            file_path = create_random_file(FILE_SIZE)
            cmds = []
            # move
            cmds.append(move_cmdlet(action, file_path))
            time.sleep(5)
            # read the file
            cmds.append(read_file(file_path))
            failed = wait_for_cmdlets(cmds)
            self.assertTrue(len(failed) == 0, "Test failed for read during {}".format(action))

    def test_mover_delete(self):
        for action in move_actions:
            file_path = create_random_file(FILE_SIZE)
            cmds = []
            # move
            cmds.append(move_cmdlet(action, file_path))
            time.sleep(5)
            # delete the file
            cmds.append(delete_file(file_path))
            failed = wait_for_cmdlets(cmds)
            self.assertTrue(len(failed) == 0, "Test failed for delete during {}".format(action))

    def test_mover_append(self):
        for action in move_actions:
            file_path = create_random_file(FILE_SIZE)
            cmds = []
            # move
            cmds.append(move_cmdlet(action, file_path))
            time.sleep(5)
            # append the file
            cmds.append(append_file(file_path,
                                    random.randrange(1024, 1024 * 1024 * 2)))
            failed = wait_for_cmdlets(cmds)
            self.assertTrue(len(failed) == 0, "Test failed for append during {}".format(action))

    def test_mover_overwrite(self):
        for action in move_actions:
            file_path = create_random_file(FILE_SIZE)
            cmds = []
            # move
            cmds.append(move_cmdlet(action, file_path))
            time.sleep(5)
            # overwrite the file
            cmds.append(create_file(file_path, 24 * 1024 * 1024))
            failed = wait_for_cmdlets(cmds)
            self.assertTrue(len(failed) == 0, "Test failed for overwrite during {}".format(action))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-size', default='1GB',
                        help="size of file, Default Value 1GB.")
    parser.add_argument('unittest_args', nargs='*')
    args, unknown_args = parser.parse_known_args()
    sys.argv[1:] = unknown_args
    print "The file size for test is {}.".format(args.size)
    FILE_SIZE = convert_to_byte(args.size)

    unittest.main()
