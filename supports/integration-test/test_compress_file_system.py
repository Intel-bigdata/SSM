#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script is used to auto-run HiBench tests with SmartFileSystem for SSM integration test.
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
import argparse
from subprocess import call
from os import path
from util import *
from test_compress_rule import run_compress_rule


def call_prepare(hibench_dir_arg, workload_arg, service):
    config = hibench_dir_arg + "/conf/" + service + ".conf"
    if path.exists(config):
        prepare = hibench_dir_arg + "/bin/workloads/" + workload_arg + "/prepare/prepare.sh"
        return call([prepare])
    else:
        print("Failed to prepare HiBench workload, please provide " + service + " config file!")
        return 1


def call_hadoop(hibench_dir_arg, workload_arg):
    work_load_script = "/bin/workloads/" + workload_arg + "/hadoop/run.sh"
    run = hibench_dir_arg + work_load_script
    call([run])


def call_spark(hibench_dir_arg, workload_arg):
    work_load_script = "/bin/workloads/" + workload_arg + "/spark/run.sh"
    run = hibench_dir_arg + work_load_script
    call([run])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Test SSM integration with HiBench.')
    parser.add_argument("-d", "--HiBenchDir", default='.', dest="HiBenchDir",
                        help="HiBench Directory, Default .")
    parser.add_argument("--debug", nargs='?', const=1, default=0, dest="debug",
                        help="print debug info, Default Value: 0")
    options = parser.parse_args()

    workloads = ['micro/wordcount', 'micro/sort', 'micro/terasort', 'sql/scan',
                 'sql/join', 'sql/aggregation', 'websearch/pagerank', 'ml/bayes']

    try:
        DEBUG = options.debug
        hibench_dir = options.HiBenchDir
    except (ValueError, SystemExit) as e:
        print("Usage: python test_compress_file_system.py -h")
    except IndexError:
        pass

    for workload in workloads:
        if DEBUG:
            print("DEBUG: Start running the workload: " + workload)

        if not call_prepare(hibench_dir, workload, "hadoop"):
            workload_name = workload.split("/")[1]
            workload_name = workload_name.capitalize()
            input_dir = "/HiBench/" + workload_name + "/Input"
            if DEBUG:
                print("DEBUG: input directory: " + input_dir)
            rid = run_compress_rule(input_dir, DEBUG)
            call_hadoop(hibench_dir, workload)
        else:
            print("Failed to run " + workload)
