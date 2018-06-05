#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script will be used to create test data set. It is also called by:
- test_small_file_rule.py
- test_small_file_actions.py
"""
import sys
import ast
import os
import re
import argparse
from util import *


def create_test_set(file_set_nums, file_size, base_dir, debug):
    created_files = []
    cids = []
    size_in_byte = file_size * 1024
    for i in file_set_nums:
        if debug:
            print("DEBUG: Current file set number: " + str(i) + "; each file size: " + str(file_size) + "KB")
        created_files_dir = base_dir + os.sep + "data_" + str(i)
        for j in range(0, i):
            file_name = created_files_dir + os.sep + "file_" + str(j)
            aid = create_file(file_name, size_in_byte)
            cids.append(aid)
            created_files.append("'" + file_name + "'")
            if debug:
                print("**********Action " + str(aid) + " Submitted**********")
    wait_for_cmdlets(cids)
    time.sleep(1)
    return "[" + ','.join(created_files) + "]"


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description='Generate test data set for SSM.')
    parser.add_argument("-b", "--dataSetNums", default='[10]', dest="dataSetNums",
                        help="file number of test data sets, string input, e.g. '[10,100,1000]', Default Value: [10].")
    parser.add_argument("-s", "--fileSize", default='1MB', dest="fileSize",
                        help="size of each file, e.g. 10MB, 10KB, default unit KB, Default Value 1KB.")
    parser.add_argument("-d", "--testDir", default=TEST_DIR, dest="testDir",
                        help="Test data set directory, Default Value: TEST_DIR in util.py")
    parser.add_argument("--debug", nargs='?', const=1, default=0, dest="debug",
                        help="print debug info, Default Value: 0")
    options = parser.parse_args()

    # Convert arguments
    try:
        DEBUG = options.debug
        data_set_nums = ast.literal_eval(options.dataSetNums)
        file_size_arg = options.fileSize
        m = re.match(r"(\d+)(\w{2}).*", file_size_arg)
        if m:
            size = int(m.group(1))
            sizeUnit = m.group(2)
            if sizeUnit != "MB" and sizeUnit != "KB":
                print("Wrong Size Unit")
                print("Usage: python3 ssm_generate_test_data -h")
                sys.exit(1)
            if sizeUnit == "MB":
                size = size * 1024
        else:
            print("Wrong Size Input, e.g. 1MB or 1KB")
            sys.exit(1)

        if options.testDir:
            if options.testDir[-1:len(options.testDirPre)] == '/':
                test_dir_prefix = options.testDir[:-1]
            else:
                test_dir_prefix = options.testDir
        else:
            raise SystemExit

        if DEBUG:
            print("DEBUG: file set nums: " + options.fileSetNums + ", each file size: " + str(size) + sizeUnit
                  + ", test data directory prefix: " + test_dir_prefix)
    except (ValueError, SystemExit) as e:
        print("Usage: python3 ssm_generate_test_data -h")
    except IndexError:
        pass

    create_test_set(data_set_nums, size, test_dir_prefix, DEBUG)
