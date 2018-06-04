#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script is used to generate and submit a SSM action.
"""
import sys
import os
import argparse
import re
import ast
from subprocess import call

from util import *
from test_generate_test_set import create_test_set

def add_compact_action(targetFiles, containerFile, DEBUG):
    if DEBUG:
        print("**********Adding Compact Action**********")
    if DEBUG:
        print("DEBUG: target files: " + str(targetFiles))
    cid = compact_small_file(targetFiles, containerFile)
    if DEBUG:
        print("DEBUG： Action with ID " + str(cid) + " submitted")
    print("**********Compact Action Added**********")

def add_uncompact_action(containerFile, DEBUG):
    if DEBUG:
        print("**********Adding Uncompact Action**********")
    cid = uncompact_small_file(containerFile)
    if DEBUG:
        print("Action with ID " + str(cid) + " submitted")
    print("**********Uncompact Action Added**********")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Test SSM action submit functions for compact and uncompact.')
    parser.add_argument("-d", "--targetDir", default=TEST_DIR, dest="targetDir", 
                        help="directory to store generated test set, DefaultValue: TEST_DIR in util.py")
    parser.add_argument("-f", "--targetFiles", dest="targetFiles", 
                        help="a string contains files to compact, No Default Value, e.g. ['/dir/file1','/dir/file2']")
    parser.add_argument("-b", "--sizeOfBatches", default='[5]', dest="sizeOfBatches",
                        help="size of each batch, string input, e.g. '[10,100,1000]', Default Value: [5].")
    parser.add_argument("-s", "--sizeOfFiles", default='1MB', dest="sizeOfFiles",
                        help="size of each file, e.g. 10MB, 10KB, default unit KB, Default Value 1KB.")
    parser.add_argument("-a", "--action", dest="action", default="compact", help="action type to submit, Default Value: \"compact\"")
    parser.add_argument("-c", "--containerFile", default="/container_tmp_file", dest="containerFile", 
                        help="containerFile directory. DefaultValue: /container_tmp_file")
    parser.add_argument("--nogen", nargs='?', const=1, default=0, dest="notGenerate",
                        help="do not generate test set data flag.")
    parser.add_argument("--debug", nargs='?', const=1, default=0, dest="debug",
                        help="print debug info.")
    options = parser.parse_args()
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

        if options.targetDir[-1:len(options.targetDir)] == '/':
            targetDir = options.targetDir[:-1]
        else:
            targetDir = options.targetDir
        action = options.action
        containerFile = options.containerFile
        if DEBUG:
            print("DEBUG: nums: " + options.sizeOfFiles + ", size: " + str(size) + sizeUnit
                + ", targetDir: " + targetDir)
    except (ValueError, SystemExit) as e:
        print("Usage: python3 test_smallfile_actioin -h")
    except IndexError:
        pass
    
    if options.targetFiles:
        targetFiles = options.targetFiles
    else:
        if (action != "uncompact" and not notGen):
            targetFiles = create_test_set(nums, size, targetDir, DEBUG)
            print("Sleep 5s for syncing with DB...")
            time.sleep(5)
    if action == "compact":
        if targetFiles:
            add_compact_action(targetFiles, containerFile, DEBUG)
        else:
            print("Target files does not specified!\nUsage: python3 test_smallfile_action.py -h")
            sys.exit(1)
    elif action == "uncompact":
        if not call(['hdfs','dfs','-test','-e',containerFile]):
            add_uncompact_action(containerFile, DEBUG)
        else:
            print("Container file does not exist!")
            sys.exit(1)
    else:
        print("Unsupported action!")