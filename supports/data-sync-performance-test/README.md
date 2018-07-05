# Performance Test for Data Sync

## Requirements
- Deploy SSM, please refer to /SSM/doc/ssm-deployment-guide.md.
- Deploy two HDFS clusters and configure its bin in $PATH of OS.
- Install MySQL for SSM storing Metadata.
- Install PAT(https://github.com/intel-hadoop/PAT).

## Configuration
  Configure the file named config. For the test case, the corresponding files should be created in source cluster beforehand.

## SSM sync test
  1. Run `./test_ssm_sync_performance.sh`
  2. A file named ssm.log under this directory will record the sync time for each round of test. And ssm log and PAT data will be collected in ${PAT_HOME}/PAT-collecting-data/results.

## HDFS distcp test
  1. Yarn should be launched for the test cluster.
  2. Run `./test_distcp_performance.sh`
  3. A file named distcp.log under this directory will the record the sync time. The distcp logs and PAT data will be collected in ${PAT_HOME}/PAT-collecting-data/results.
