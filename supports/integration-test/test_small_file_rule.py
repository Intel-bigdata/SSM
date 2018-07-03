#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script is used to generate and submit SSM small file rule.
"""
import sys
import os
import argparse
import re
from util import *
from ssm_generate_test_data import create_test_set


def run_small_file_rule(small_file_dir, debug):
    # Create compact rule
    rule_str = "file : path matches " + \
        "\"" + small_file_dir + os.sep + "*\" | compact"
    if debug:
        print("DEBUG: **********Submitting Compact Rule**********")
    rid = submit_rule(rule_str)
    if debug:
        print("DEBUG: Rule with ID " + str(rid) + " submitted")

    # Activate rule
    start_rule(rid)
    if debug:
        print("DEBUG: Rule with ID " + str(rid) + " started")

    # Wait for submitting actions by rule
    cmdlets = get_cmdlets_of_rule(rid)
    count = 0
    while not cmdlets and count < 30:
        if debug:
            print("DEBUG: sleep 1s to wait for action submission")
        time.sleep(1)
        count += 1
        cmdlets = get_cmdlets_of_rule(rid)

    # Check if every action is DONE
    if cmdlets:
        if debug:
            print("DEBUG: get generated cmdlets of new submitted rule")
        cids = get_cids_of_rule(rid)
        failed = wait_for_cmdlets(cids)
        if len(failed) != 0:
            for cid in failed:
                print("Failed to execute the cmdlet: id = " + str(cid))
        print("All actions execute successfully")
    else:
        print("No cmdlet is generated")
    stop_rule(rid)


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description='Auto-generate and submit compact rules.')
    parser.add_argument("-d", "--testDir", default=TEST_DIR, dest="testDir",
                        help="target test set directory Prefix, Default Value: TEST_DIR in util.py")
    parser.add_argument("-n", "--fileNum", default='5', dest="fileNum",
                        help="number of small files, string input, e.g. '10', Default Value: 5.")
    parser.add_argument("-s", "--fileSize", default='1MB', dest="fileSize",
                        help="size of each small file, e.g. 10MB, 10KB, default unit KB, Default Value 1KB.")
    parser.add_argument("--noGen", nargs='?', const=1, default=0, dest="notGenerate",
                        help="do not generate test set data flag.")
    parser.add_argument("--debug", nargs='?', const=1, default=0, dest="debug",
                        help="print debug info.")
    options = parser.parse_args()

    # Convert arguments
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

        if options.testDir:
            if options.testDir[-1:len(options.testDir)] == '/':
                test_dir = options.testDir[:-1]
            else:
                test_dir = options.testDir
        else:
            raise SystemExit

        if DEBUG:
            print("DEBUG: small file number: " + file_num + ", file size: " + str(file_size) + sizeUnit
                  + ", test small files directory: " + test_dir)
    except (ValueError, SystemExit) as e:
        print("Usage: python3 test_small_file_rule.py -h")
    except IndexError:
        pass

    if notGen:
        print("Please make sure there are small files in the test data directory: "
              + test_dir + "/data_" + file_num)
    else:
        create_test_set([int(file_num)], file_size, test_dir, DEBUG)
    run_small_file_rule(test_dir + "/data_" + file_num, DEBUG)
