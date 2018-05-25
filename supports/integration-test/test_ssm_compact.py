#!/usr/bin/env python3
"""
This script is used to generate test data set with the help of TestDFSIO.
After test data set created, an SSM rule will be automatically generated and added.
"""
import sys
import ast
import os
import re
import argparse
import glob
from subprocess import call

from util import *

def add_ssm_rule(targetDir):
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


def create_file_DFSIO(nums, size, targetDir, isCompact):
    for i in nums:
        if DEBUG:
            print("DEBUG: Current batch num: " + str(i) + "; each file size: " + str(size))
        jarDirPre = os.environ['HADOOP_HOME'] + "/share/hadoop/mapreduce" + "/hadoop-mapreduce-client-jobclient-*-tests.jar"
        jarDirList = glob.glob(jarDirPre)
        if jarDirList:
            jarDir = jarDirList[0]
        dfsio_cmd = ["hadoop",
                     "jar",
                     jarDir,
                     "TestDFSIO",
                     "-write", 
                     "-nrFiles",
                     str(i),
                     "-fileSize",
                     str(size) + "MB"]
        call(dfsio_cmd)
        if (call(['hdfs','dfs','-test','-e',targetDir])):
            call(['hdfs','dfs','-mkdir','-p',targetDir])
        else:
            call(['hdfs','dfs','-rm','-r',targetDir + os.sep + '*'])
        if (not call(["hdfs","dfs","-mv","/benchmarks/TestDFSIO/io_data",targetDir + os.sep + "data_" + str(i)])):
            print("**********Create Test File Success**********\n")
            if isCompact:
                targetDir = targetDir + os.sep + "data_" + str(i)
                add_ssm_rule(targetDir)
        else:
            print("**********Create Test File ERROR**********\nmv Failed\n**********ERROR END**********\n")



if __name__ == '__main__':
    # Parse Arguments
    parser = argparse.ArgumentParser(description='Generate test data set for SSM and add corresponding compact rules.')
    parser.add_argument("-b", "--sizeOfBatches", default='[10]', dest="sizeOfBatches",
                    help="size of each batch, string input, e.g. '[10,100,1000]', Default Value: [10].")
    parser.add_argument("-s", "--sizeOfFiles", default='1MB', dest="sizeOfFiles",
                    help="size of each file, e.g. 10MB, 10KB, default unit MB, Default Value 1MB.")
    parser.add_argument("-d", "--testDirPre", default='/ssm_tmp_test',dest="testDirPre",
                    help="target test set directory Prefix, Default Value: /ssm_tmp_test")
    parser.add_argument("-c", "--ssmCompact", dest="ssmCompact", action='store_true',
                    help="add compact rules to all new generated directory")
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
                print("Wrong Size Unit\nUsage: python3 test_ssm_compact -h")
                sys.exit(1)
        else:
            print("Wrong Size Input, e.g. 1MB")
            sys.exit(1)
        if options.testDirPre:
            testDirPre = options.testDirPre
        else:
            raise SystemExit
        isCompact = options.ssmCompact
        if DEBUG:
            print("DEBUG: nums: ", end='')
            print(nums, end='')
            print(", size: " + str(size) + sizeUnit
                + ", testDirPre: "+ testDirPre
                + ", isCompact: ", end='')
            print(isCompact)
    except (ValueError, SystemExit) as e:
        print("Usage: python3 test_ssm_compact -h")
    except IndexError:
        pass
    
    try:
        create_file_DFSIO(nums, size, testDirPre, isCompact)
    except NameError:
        pass