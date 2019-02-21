#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script is used to generate and submit a SSM compress action.
"""
import sys
import argparse
import re
import ast
from subprocess import call
from util import *
from ssm_generate_test_data import create_test_set


def run_compress_actions(test_file_list, debug):
    if debug:
        print("**********Submitting Compress Action**********")
        print("DEBUG: compress files: " + str(test_file_list))
    file_list = ast.literal_eval(test_file_list)
    cids = []
    for f in file_list:
        cid = compress_file(f)
        if debug:
            print("DEBUG：Action with ID " + str(cid) + " submitted.")
        cids.append(cid)
    time.sleep(1)
    failed = wait_for_cmdlets(cids)
    if len(failed) == 0:
        print("Compress action executes successfully")
    else:
        print("Failed to execute compress action")


def run_compress_action(file_path, debug):
    if debug:
        print("**********Submitting Compress Action**********")
        print("DEBUG: compress files: " + file_path)
    cid = compress_file(file_path)
    if debug:
        print("DEBUG：Action with ID " + str(cid) + " submitted.")
    wait_for_cmdlet(cid)
    if get_cmdlet(cid)['state'] == "DONE":
        print("Compress action executes successfully")
    else:
        print("Failed to execute compress action")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Test SSM compress actions.')
    parser.add_argument("-d", "--testDir", default=TEST_DIR, dest="testDir",
                        help="directory to store generated test set, DefaultValue: TEST_DIR in util.py")
    parser.add_argument("-f", "--Files", dest="Files",
                        help="a string contains test files to be compressed,"
                             " No Default Value, e.g. ['/dir/file1','/dir/file2']")
    parser.add_argument("-n", "--fileNum", default='5', dest="fileNum",
                        help="number of test files, string input, e.g. '10', Default Value: 5.")
    parser.add_argument("-s", "--fileSize", default='10MB', dest="fileSize",
                        help="size of each test file, e.g. 10MB, 10KB, default unit KB, Default Value 1KB.")
    parser.add_argument("-a", "--action", dest="action", default="compress",
                        help="action type to submit, Default Value: \"compress\"")
    parser.add_argument("--debug", nargs='?', const=1, default=0, dest="debug",
                        help="print debug info.")
    options = parser.parse_args()
    try:
        DEBUG = options.debug
        file_num = options.fileNum

        # Get compress file size
        file_size_arg = re.match(r"(\d+)(\w{2}).*", options.fileSize)
        if file_size_arg:
            file_size = int(file_size_arg.group(1))
            sizeUnit = file_size_arg.group(2)
            if sizeUnit != "MB" and sizeUnit != "KB":
                print("Wrong Size Unit.")
                print("Usage: python test_compress_file_actions -h")
                sys.exit(1)
            if sizeUnit == "MB":
                file_size = file_size * 1024
        else:
            print("Wrong file size input, e.g. 1MB or 1KB")
            sys.exit(1)

        if options.testDir[-1:len(options.testDir)] == '/':
            test_dir = options.testDir[:-1]
        else:
            test_dir = options.testDir

        action = options.action
        if DEBUG:
            print("DEBUG: compress file number: " + file_num + ", file size: " + str(file_size) + sizeUnit
                  + ", test compress files directory: " + test_dir)
    except (ValueError, SystemExit) as e:
        print("Usage: python test_compress_actions -h")
    except IndexError:
        pass

    if options.Files:
        test_files = options.Files
    else:
        test_files = create_test_set([int(file_num)], file_size, test_dir, DEBUG)
        print("Sleep 5s to wait for syncing with DB...")
        time.sleep(5)

    if action == "compress":
        if test_files:
            run_compress_actions(test_files, DEBUG)
        else:
            print("Test file list is not specified!")
            print("Usage: python test_compress_actions.py -h")
            sys.exit(1)
    else:
        print("Unsupported action!")
