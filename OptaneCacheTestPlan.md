Optane Cache Test Plan
=========================

Purpose
-----------------

* Verify that Optane can serve as memory for HDFS to expand the memory size for cache
* By HDFS cache with Optane, HDFS reading operation can be accelerated
* Compare the performance of cache with Optane to cache with DRAM

Test Environment
-----------------

* Cluster： 1 Namenode + 2 Datanodes
* Disk： 5 HDDs on each datanode
* Memory : 375 GB
* Optane : 375 GB
* Hadoop version: trunk (3.0.0-beta1-SNAPSHOT)

Workload
-----------------

Use ErasureCodeBenchmarkThroughput tool to measure the reading performance.
#### DRAM alone
* 300 GB uncached data
* 300 GB cached data
#### DRAM + Optane
* 600 GB uncached data
* 600 GB cached data


Performance Measurement
-----------------

* End-to-end execution time
* Throughput of the reading workload
* Disk bandwidth








