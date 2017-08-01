Optane Cache Test Plan
=========================

Purpose
-----------------

* Verify that Optane can serve as memory for HDFS to expand the memory size for cache
* By HDFS cache with Optane, HDFS read operation can be accelerated
* Compare the performance of cache with Optane to that of cache with DRAM

Test Environment
-----------------

* Cluster： 1 Namenode + 2 Datanodes
* Disk： 5 HDDs on each datanode
* Hadoop version: trunk (3.0.0-beta1-SNAPSHOT)

Workload
-----------------

* ErasureCodeBenchmarkThroughput read
* Data set size: 100GB
* 

Performance Measurement
-----------------

* End-to-end execution time
* Disk bandwith, disk latency
* CPU utilization
* Network IO








