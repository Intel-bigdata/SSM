import argparse
import timeout_decorator
import unittest
from util import *


@timeout_decorator.timeout(seconds=200)
def verify(rid, num_cmds_gen):
    while True:
        time.sleep(1)
        rule = get_rule(rid)
        if rule['numCmdsGen'] >= num_cmds_gen:
            break


class TestSync(unittest.TestCase):
    def test_sync_rule(self):
        file_paths = []
        cids = []
        # create a directory with random name
        sync_dir = TEST_DIR + random_string() + "/"
        # create random files in the above directory
        for i in range(MAX_NUMBER):
            file_path, cid = create_random_file_parallel(FILE_SIZE, sync_dir)
            file_paths.append(file_path)
            cids.append(cid)
        wait_for_cmdlets(cids)

        # wait for DB sync
        time.sleep(5)
        rule_str = "file : every 1s | path matches " + \
                   "\"" + sync_dir + "*\" | sync -dest " + DEST_DIR
        rid = submit_rule(rule_str)
        start_rule(rid)
        # Status check
        num_cmds_gen = MAX_NUMBER
        verify(rid, num_cmds_gen)
        cids = get_cids_of_rule(rid)
        failed = wait_for_cmdlets(cids)
        self.assertTrue(len(failed) == 0)

        # test delete src file
        # DB sync
        time.sleep(5)
        cids = []
        num_delete = random.randrange(len(file_paths))
        num_cmds_gen = num_cmds_gen + num_delete
        for i in range(num_delete):
            cids.append(submit_cmdlet("delete -file " + file_paths[i]))
        wait_for_cmdlets(cids)
        verify(rid, num_cmds_gen)

        # test create src file
        cids = []
        num_create = 10
        num_cmds_gen = num_cmds_gen + num_create
        for i in range(num_create):
            file_path, cid = create_random_file_parallel(FILE_SIZE, sync_dir)
            cids.append(cid)
        wait_for_cmdlets(cids)
        verify(rid, num_cmds_gen)
        failed = wait_for_cmdlets(get_cids_of_rule(rid))
        self.assertTrue(len(failed) == 0)

        # test subdir case
        cids = []
        subdir = sync_dir + random_string() + "/"
        num_subdir_files = 10
        num_cmds_gen = num_cmds_gen + num_subdir_files
        for i in range(num_subdir_files):
            file_path, cid = create_random_file_parallel(FILE_SIZE, subdir)
            file_paths.append(file_path)
            cids.append(cid)
        wait_for_cmdlets(cids)
        verify(rid, num_cmds_gen)
        failed = wait_for_cmdlets(get_cids_of_rule(rid))
        self.assertTrue(len(failed) == 0)
        # delete subdir
        cid = submit_cmdlet("delete -file " + subdir)
        wait_for_cmdlet(cid)
        num_cmds_gen = num_cmds_gen + 1
        verify(rid, num_cmds_gen)
        failed = wait_for_cmdlets(get_cids_of_rule(rid))
        self.assertTrue(len(failed) == 0)

        # test rename case
        cids = []
        num_rename = 10
        src_rename = []
        num_cmds_gen = num_cmds_gen + num_rename
        for i in range(num_rename):
            file_path, cid = create_random_file_parallel(FILE_SIZE, sync_dir)
            src_rename.append(file_path)
            cids.append(cid)
        wait_for_cmdlets(cids)
        # sync original files
        verify(rid, num_cmds_gen)
        failed = wait_for_cmdlets(get_cids_of_rule(rid))
        self.assertTrue(len(failed) == 0)
        cids = []
        for i in range(num_rename):
            cids.append(submit_cmdlet("rename -file " + src_rename[i] + " -dest " + sync_dir + random_string()))
        wait_for_cmdlets(cids)
        num_cmds_gen = num_cmds_gen + num_rename
        # sync renamed file
        verify(rid, num_cmds_gen)
        failed = wait_for_cmdlets(get_cids_of_rule(rid))
        self.assertTrue(len(failed) == 0)

        # TODO: test rename from outside dir into sync dir
        # TODO: rename from sync dir to outside dir
        # TODO: rename the syncing file

        time.sleep(5)
        stop_rule(rid)


if __name__ == '__main__':
    requests.adapters.DEFAULT_RETRIES = 5
    s = requests.session()
    s.keep_alive = False

    parser = argparse.ArgumentParser()
    parser.add_argument('-size', default='1MB',
                        help="size of file, Default Value 1MB.")
    parser.add_argument('-num', default='50',
                        help="file num, Default Value 50.")
    # To sync files to another cluster, please use "-dest hdfs://hostname:port/dest/"
    parser.add_argument('-dest', default='/dest/',
                        help="directory to store generated test set, DefaultValue: /dest/")
    parser.add_argument('unittest_args', nargs='*')
    args, unknown_args = parser.parse_known_args()
    sys.argv[1:] = unknown_args
    print "The file size for test is {}.".format(args.size)
    FILE_SIZE = convert_to_byte(args.size)
    print "The file number for test is {}.".format(args.num)
    MAX_NUMBER = int(args.num)
    print "The dest directory for test is {}.".format(args.dest)
    DEST_DIR = args.dest

    unittest.main()
