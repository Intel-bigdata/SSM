# Performance Test for SSM Mover
Test the performance of SSM Mover and compare it with HDFS Mover. We user mover to migrate data from all_disk to all_ssd. You can also choose other storage policies to test the mover performance.

## Requirements
- Deploy SSM, please refer to /SSM/doc/ssm-deployment-guide.md.
- Deploy one HDFS cluster and configure its bin in $PATH of OS.
- Install PAT(https://github.com/intel-hadoop/PAT).

## Configuration
  Configure the file named `config`. For a test case, the corresponding test data should be created in the HDFS cluster beforehand by executing 'prepare.sh'.

## Compression Test
  1. Run `./test_hdfs_mover_performance.sh` and `./test_ssm_mover_performance.sh`
  2. A file named ssm.log under this directory will record the time for each round of test. SSM log and PAT data will be collected in `${PAT_HOME}/PAT-collecting-data/results`.


## Notes
- You should configure `dfs.datanode.data.dir` and HDFS HSM related configuration before you run this test. For example, you can set datanode path as `/hadoop/hdfs/data1,[SSD]/hadoop/hdfs/data2`.