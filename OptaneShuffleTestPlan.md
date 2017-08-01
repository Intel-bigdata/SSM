Optane Shuffle Test Plan
=========================

Purpose
-----------------

* Optane serves as shuffle disk for Hadoop 
* Compare with HDD and P3700 as shuffle


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








