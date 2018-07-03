#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script is used to generate and submit a SSM small file action.
"""
import sys
import argparse
import re
from subprocess import call
from util import *
from ssm_generate_test_data import create_test_set


def run_compact_action(small_file_list, container_file_name, debug):
    if debug:
        print("**********Submitting Compact Action**********")
        print("DEBUG: small files: " + str(small_file_list))
    cid = compact_small_file(small_file_list, container_file_name)
    if debug:
        print("DEBUG：Action with ID " + str(cid) + " submitted.")
    wait_for_cmdlet(cid)
    if get_cmdlet(cid)['state'] == "DONE":
        print("Compact action executes successfully")
    else:
        print("Failed to execute compact action")


def run_uncompact_action(container_file_name, debug):
    if debug:
        print("**********Submitting Uncompact Action**********")
        print("DEBUG: container file: " + str(container_file))
    cid = uncompact_small_file(container_file_name)
    if debug:
        print("Action with ID " + str(cid) + " submitted")
    wait_for_cmdlet(cid)
    if get_cmdlet(cid)['state'] == "DONE":
        print("Uncompact action executes successfully")
    else:
        print("Failed to execute uncompact action")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Test SSM small file compact and uncompact actions.')
    parser.add_argument("-d", "--testDir", default=TEST_DIR, dest="testDir",
                        help="directory to store generated test set, DefaultValue: TEST_DIR in util.py")
    parser.add_argument("-f", "--smallFiles", dest="smallFiles",
                        help="a string contains small files to be compacted,"
                             " No Default Value, e.g. ['/dir/file1','/dir/file2']")
    parser.add_argument("-n", "--fileNum", default='5', dest="fileNum",
                        help="number of small files, string input, e.g. '10', Default Value: 5.")
    parser.add_argument("-s", "--fileSize", default='1MB', dest="fileSize",
                        help="size of each small file, e.g. 10MB, 10KB, default unit KB, Default Value 1KB.")
    parser.add_argument("-a", "--action", dest="action", default="compact",
                        help="action type to submit, Default Value: \"compact\"")
    parser.add_argument("-c", "--containerFile", default="/_container_tmp_file", dest="containerFile",
                        help="container file name. DefaultValue: /container_tmp_file")
    parser.add_argument("--noGen", nargs='?', const=1, default=0, dest="notGenerate",
                        help="do not generate test set data flag.")
    parser.add_argument("--debug", nargs='?', const=1, default=0, dest="debug",
                        help="print debug info.")
    options = parser.parse_args()
    try:
        DEBUG = options.debug
        file_num = options.fileNum
        notGen = options.notGenerate

        # Get small file size
        file_size_arg = re.match(r"(\d+)(\w{2}).*", options.fileSize)
        if file_size_arg:
            file_size = int(file_size_arg.group(1))
            sizeUnit = file_size_arg.group(2)
            if sizeUnit != "MB" and sizeUnit != "KB":
                print("Wrong Size Unit.")
                print("Usage: python3 test_small_file_actions -h")
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
        container_file = options.containerFile
        if DEBUG:
            print("DEBUG: small file number: " + file_num + ", file size: " + str(file_size) + sizeUnit
                  + ", test small files directory: " + test_dir)
    except (ValueError, SystemExit) as e:
        print("Usage: python3 test_small_file_actions -h")
    except IndexError:
        pass

    if options.smallFiles:
        small_files = options.smallFiles
    else:
        if action != "uncompact" and not notGen:
            small_files = create_test_set([int(file_num)], file_size, test_dir, DEBUG)
            print("Sleep 5s to wait for syncing with DB...")
            time.sleep(5)

    if action == "compact":
        if small_files:
            run_compact_action(small_files, container_file, DEBUG)
        else:
            print("Small file list is not specified!")
            print("Usage: python3 test_small_file_actions.py -h")
            sys.exit(1)
    elif action == "uncompact":
        if not call(['hdfs', 'dfs', '-test', '-e', container_file]):
            run_uncompact_action(container_file, DEBUG)
        else:
            print("Container file is not exist!")
            sys.exit(1)
    else:
        print("Unsupported action!")
