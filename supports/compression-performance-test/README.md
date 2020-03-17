# Performance Test for Compression
Test performance before and after file compression. In order to better illustrate the performance, TestDFSIO and Terasort tests will be performed.

## Requirements
- Deploy SSM, please refer to /SSM/doc/ssm-deployment-guide.md.
- Deploy one HDFS cluster and configure its bin in $PATH of OS.
- Install HiBench 7.0(https://github.com/Intel-bigdata/HiBench).
- Install PAT(https://github.com/intel-hadoop/PAT).

## Configuration
  Configure the file named `config`. For a test case, the corresponding test data should be created in the HDFS cluster beforehand by executing 'prepare.sh'.

## Compression Test
  1. Run `./test_compression_performance.sh`
  2. A file named ssm.log under this directory will record the time for each round of test. SSM log and PAT data will be collected in `${PAT_HOME}/PAT-collecting-data/results`.


## Notes
- You should configure HiBench before you run this test. You can config terasort in `${HiBench_HOME}/conf/workloads/micro/terasort.conf`.
- You can check the result of tersort in the default path `${HiBench_HOME}/report`.