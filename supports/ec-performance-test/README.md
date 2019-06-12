# Performance Test for Data Sync

## Requirements
- Deploy SSM, please refer to /SSM/doc/ssm-deployment-guide.md.
- Deploy one HDFS cluster and configure its bin in $PATH of OS.
- Install MySQL for SSM storing Metadata.
- Install PAT(https://github.com/intel-hadoop/PAT).

## Configuration
  Configure the file named config. For the test case, the corresponding test data should be created in the HDFS cluster beforehand by executing 'prepare.sh'.

## SSM EC test
  1. Run `./test_ssm_ec_performance.sh`
  2. A file named ssm.log under this directory will record the time for each round of test. SSM log and PAT data will be collected in ${PAT_HOME}/PAT-collecting-data/results.
  Note: The rule check interval in run_ssm_ec.py was set to a long period, to ensure the rule check was conducted only once during test. So that a large amount of redundant cmdlets can be saved and the execution time becomes more accurate.
## HDFS Distcp EC test
  1. Yarn should be launched for the test cluster.
  2. Run `./test_distcp_ec.sh`
  3. A file named distcp.log under this directory will record the time. The distcp logs and PAT data will be collected in ${PAT_HOME}/PAT-collecting-data/results.

## Other test scripts
  1. The script 'test_ssm_ec_only.sh' is used to test ssm ec for 1 time, without unec operation.
  2. The script 'test_ssm_unec_only.sh' is used to test ssm unec for 1 time, without ec operation.
  3. The script 'test_distcp_replica.sh' is used to copy the files which are converted to ec policy alreadly to a directory whose ec policy is set as 3 replica.

## Note
- For the sake of fair comparison, DistCP and SSM should have the same parallelism. That means the num of mappers for Distcp should be consistent with the num of overall executors (smart.cmdlet.executors * num_smart_agent) for SSM.
In our test, the parallelism is 90. For Distcp, this value is specified in cmd. For SSM, given that there are 9 smart agents, smart.cmdlet.executors in smart-default.xml should be set as 10.
And smart.action.local.execution.disabled should be set as true, thus smart server will not be used to execute tasks.
- You should drop cache after each round of test to avoid impact from OS cache. This operation can be included in your test scripts.