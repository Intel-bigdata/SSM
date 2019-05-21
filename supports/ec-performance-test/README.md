# Performance Test for Data Sync

## Requirements
- Deploy SSM, please refer to /SSM/doc/ssm-deployment-guide.md.
- Deploy two HDFS clusters and configure its bin in $PATH of OS.
- Install MySQL for SSM storing Metadata.
- Install PAT(https://github.com/intel-hadoop/PAT).

## Configuration
  Configure the file named config. For the test case, the corresponding test data should be created in the HDFS cluster beforehand by executing 'prepare.sh'.

## SSM ec test
  1. Run `./test_ssm_ec_performance.sh`
  2. A file named ssm.log under this directory will record the time for each round of test. SSM log and PAT data will be collected in ${PAT_HOME}/PAT-collecting-data/results.

## HDFS distcp ec test
  1. Yarn should be launched for the test cluster.
  2. Run `./test_distcp_ec.sh`
  3. A file named distcp.log under this directory will record the time. The distcp logs and PAT data will be collected in ${PAT_HOME}/PAT-collecting-data/results.

## Other test scripts
  The script 'test_ssm_ec_only.sh' is used to test ssm ec for 1 time, without unec operation.
  The script 'test_ssm_unec_only.sh' is used to test ssm unec for 1 time, without ec operation.
  The script 'test_distcp_replica.sh' is used to copy the files which are converted to ec policy alreadly to a dir whose ec policy is set as 3 replica.
