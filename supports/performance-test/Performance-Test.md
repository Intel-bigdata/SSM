# Performance Test
Provide two functional performance test scripts. Scripts of these two test have each own `config` file. You need to configure these files correctly before you run this test. Test performance will be recorded by PAT.

## Test Environment
- Hadoop 3.1.1.3.0.1.0-187 HDP Version
- PAT：Performance Analysis Tool
- HiBench

## Compression
Test read performance before and after file compression. In order to better illustrate the performance, TestDFSIO and Terasort tests will be performed.
You can run `test_compress_read_performance.sh` after configuration. This test will run automatically and the results will be recorded in the log. After completing the test, you can view the task running status through PAT.

## SSM Mover
Test the performance of SSM Mover and compare it with HDFS Mover. Run `test_hdfs_mover_performance.sh` and `test_ssm_mover_performance.sh` after configuration.
