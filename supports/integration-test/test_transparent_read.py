#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script is used to auto-run SSM transparent read with HiBench test for SSM integration test.
The following tests are included:
    - 'micro/wordcount', 
    - 'micro/sort', 
    - 'micro/terasort', 
    - 'ml/bayes',
    - 'sql/scan', 
    - 'sql/join', 
    - 'sql/aggregation', 
    - 'websearch/pagerank'
Modify `workloads` variable to change tests.
"""
import sys
import ast
import os
import re
import argparse
import glob
from subprocess import call
from os import path

from util import *


def call_prepare(hiBenchDir, workload, service):
    config = hiBenchDir + "/conf/" + service + ".conf"
    if path.exists(config):
        prepare = hiBenchDir + "/bin/workloads/" + workload + "/prepare/prepare.sh"
        return call([prepare])
    else:
        print("Prepare Failed, Please Provide " + service + " Config File First!")
        return 1

def call_hadoop(hiBenchDir, workload):
    wlDir = "/bin/workloads/" + workload + "/hadoop/run.sh"
    run = hiBenchDir + wlDir
    call([run])

def call_spark(hiBenchDir, workload):
    wlDir = "/bin/workloads/" + workload + "/spark/run.sh"
    run = hiBenchDir + wlDir
    call([run])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Test SSM integration with HiBench.')
    parser.add_argument("-d", "--hiBenchDir", default='.', dest="hiBenchDir",
                    help="HiBench Directory, Default .")
    parser.add_argument("--debug", nargs='?', const=1, default=0, dest="debug",
                    help="print debug info, Default Value: 0")
    options = parser.parse_args()

    workloads = ['micro/wordcount', 'micro/sort', 'micro/terasort', 'ml/bayes',
                 'sql/scan', 'sql/join', 'sql/aggregation', 'websearch/pagerank'
                ]
    try:
        DEBUG = options.debug
        hibenchDir = options.hiBenchDir
    except (ValueError, SystemExit) as e:
        print("Usage: python3 test_transparent_read.py -h")
    except IndexError:
        pass
    
    for eachWL in workloads:
        if DEBUG:
            print("DEBUG: Testing " + eachWL)
        if not call_prepare(hibenchDir, eachWL, "hadoop"):
            call_hadoop(hibenchDir, eachWL)
        else:
            print("Hadoop Task for " + eachWL + " Failed!")