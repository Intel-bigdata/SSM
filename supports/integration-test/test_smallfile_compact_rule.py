#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script is used to generate and submit a SSM compact rule.
"""
import sys
import os
import argparse
import re
import ast
from subprocess import call

from util import *
from test_generate_test_set import create_test_set

def add_ssm_rule(targetDir, DEBUG):
    if DEBUG:
        print("DEBUG: **********Adding Compact Rule**********\n")
    # Create compact rule
    rule_str = "file : path matches " + \
        "\"" + targetDir + os.sep + "*\" | compact"
    try:
        rid = submit_rule(rule_str)
    except Exception as e:
        print("**********Submit Rule ERROR**********")
        print("Connect to SSM Failed\n**********ERROR END**********\n")
    if DEBUG:
        print("DEBUG: Rule with ID " + str(rid) + " submitted\n")
    # Activate rule
    start_rule(rid)
    if DEBUG:
        print("DEBUG: Rule with ID " + str(rid) + " started\n")
    print("**********Compact Rule Added**********\n")
    return rid


if __name__ == '__main__':
    # Parse Arguments
    parser = argparse.ArgumentParser(description='Auto-generate and submit compact rules.')
    parser.add_argument("-d", "--targetDir", default=TEST_DIR,dest="targetDir",
                    help="target test set directory Prefix, Default Value: TEST_DIR in util.py")
    parser.add_argument("-b", "--sizeOfBatches", default='[10]', dest="sizeOfBatches",
                    help="size of each batch, string input, e.g. '[10,100,1000]', Default Value: [10].")
    parser.add_argument("-s", "--sizeOfFiles", default='1MB', dest="sizeOfFiles",
                    help="size of each file, e.g. 10MB, 10KB, default unit KB, Default Value 1KB.")
    parser.add_argument("--nogen", nargs='?', const=1, default=0, dest="notGenerate",
                    help="do not generate test set data flag.")
    parser.add_argument("--debug", nargs='?', const=1, default=0, dest="debug",
                    help="print debug info.")
    options = parser.parse_args()

    # Conver Arguments to values
    try:
        DEBUG = options.debug
        nums = ast.literal_eval(options.sizeOfBatches)
        sizeString = options.sizeOfFiles
        notGen = options.notGenerate
        m = re.match(r"(\d+)(\w{2}).*", sizeString)
        if m:
            sizeUnit = m.group(2)
            size = int(m.group(1))
            if sizeUnit != "MB" and sizeUnit != "KB":
                print("Wrong Size Unit\nUsage: python3 test_geberate_test_set -h")
                sys.exit(1)
            if sizeUnit == "MB":
                size = size * 1024
        else:
            print("Wrong Size Input, e.g. 1MB or 1KB")
            sys.exit(1)
        if options.targetDir:
            if options.targetDir[-1:len(options.targetDir)] == '/':
                targetDir = options.targetDir[:-1]
            else:
                targetDir = options.targetDir
        else:
            raise SystemExit
        if DEBUG:
            print("DEBUG: nums: " + options.sizeOfFiles + ", size: " + str(size) + sizeUnit
                + ", targetDir: "+ targetDir)
    except (ValueError, SystemExit) as e:
        print("Usage: python3 test_smallfile_compact_rule.py -h")
    except IndexError:
        pass
    
    if not notGen:
        create_test_set(nums, size, targetDir, DEBUG)
    add_ssm_rule(targetDir, DEBUG)