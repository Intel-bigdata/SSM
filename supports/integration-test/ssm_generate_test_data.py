#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script will be used to generate test data set. It is also called by:
- test_smallfile_compact_rule.py
- test_smallfile_action.py
"""
import sys
import ast
import os
import re
import argparse
from subprocess import call

from util import *


def create_test_set(nums, size, baseDir, DEBUG):
    createdFiles = []
    cids = []
    sizeInByte = size * 1024
    for i in nums:
        if DEBUG:
            print("DEBUG: Current batch num: " + str(i) + "; each file size: " + str(size) + "KB")
        targetDir = baseDir + os.sep + "data_" + str(i)
        try:
            # delete old target directory
            if DEBUG:
                print("Deleting Old Target Directory " + targetDir)
            cmdlet = wait_for_cmdlet(delete_file(targetDir))
            if DEBUG:
                print("Old Directory Deleted with return " + str(cmdlet))
        except Exception:
            pass
        for j in range(0,i):
            fileName = targetDir + os.sep + "file_" + str(j)
            aid = create_file(fileName, sizeInByte)
            cids.append(aid)
            createdFiles.append("'" + fileName + "'")
            if DEBUG:
                print("**********Action " + str(aid) + " Created**********")
    wait_for_cmdlets(cids)
    return "[" + ','.join(createdFiles) + "]"

if __name__ == '__main__':
    # Parse Arguments
    parser = argparse.ArgumentParser(description='Generate test data set for SSM and add corresponding compact rules.')
    parser.add_argument("-b", "--sizeOfBatches", default='[10]', dest="sizeOfBatches",
                    help="size of each batch, string input, e.g. '[10,100,1000]', Default Value: [10].")
    parser.add_argument("-s", "--sizeOfFiles", default='1MB', dest="sizeOfFiles",
                    help="size of each file, e.g. 10MB, 10KB, default unit KB, Default Value 1KB.")
    parser.add_argument("-d", "--testDirPre", default=TEST_DIR,dest="testDirPre",
                    help="target test set directory Prefix, Default Value: TEST_DIR in util.py")
    parser.add_argument("--debug", nargs='?', const=1, default=0, dest="debug",
                    help="print debug info, Default Value: 0")


    options = parser.parse_args()

    # Conver Arguments to values
    try:
        DEBUG = options.debug
        nums = ast.literal_eval(options.sizeOfBatches)
        sizeString = options.sizeOfFiles
        m = re.match(r"(\d+)(\w{2}).*", sizeString)
        if m:
            sizeUnit = m.group(2)
            size = int(m.group(1))
            if sizeUnit != "MB" and sizeUnit != "KB":
                print("Wrong Size Unit\nUsage: python3 ssm_generate_test_data -h")
                sys.exit(1)
            if sizeUnit == "MB":
                size = size * 1024
        else:
            print("Wrong Size Input, e.g. 1MB or 1KB")
            sys.exit(1)
        if options.testDirPre:
            if options.testDirPre[-1:len(options.testDirPre)] == '/':
                testDirPre = options.testDirPre[:-1]
            else:
                testDirPre = options.testDirPre
        else:
            raise SystemExit
        if DEBUG:
            print("DEBUG: nums: " + options.sizeOfFiles + ", size: " + str(size) + sizeUnit
                + ", testDirPre: "+ testDirPre)
    except (ValueError, SystemExit) as e:
        print("Usage: python3 ssm_generate_test_data -h")
    except IndexError:
        pass
    
    create_test_set(nums, size, testDirPre, DEBUG)