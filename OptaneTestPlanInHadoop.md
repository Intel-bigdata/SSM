Optane Test Plan in Hadoop
==============

Use Optane as HDFS Cache
-----------------

### 1. Purpose

* Verify that Optane can serve as memory for HDFS to expand the memory size for cache
* By HDFS cache with Optane, HDFS reading operation can be accelerated
* Compare the performance of cache with Optane to cache with DRAM

### 2. Test Environment

* Cluster： 1 Namenode + 2 Datanodes
* Disk： 5 HDDs on each datanode
* Memory : 375 GB
* Optane : 375 GB
* Hadoop version: trunk (3.0.0-beta1-SNAPSHOT)

### 3. Benchmark

Use ErasureCodeBenchmarkThroughput tool to measure the reading performance. Each test case will run in two modes: sequential read and random read.
#### DRAM alone
* 300 GB uncached data
* 300 GB cached data
#### DRAM + Optane
* 600 GB uncached data
* 600 GB cached data


### 4. Performance Measurement

* End-to-end execution time
* Throughput of the reading workload
* Disk bandwidth


Use Optane as Shuffle
-----------------

### 1. Purpose

* Optane serves as shuffle disk for Hadoop 
* Compare with HDD and P3700 as shuffle


### 2. Test Environment

* Cluster： 1 Namenode + 2 Datanodes
* HDFS Disk： 5 HDDs on each datanode
* Memory : 100 GB
* Shuffle Disk : 1 HDD / 1 P3700 / 1 Optane
* Hadoop version: trunk (3.0.0-beta1-SNAPSHOT)

### 3. Workload

For each workload, three experiments will be done using HDD, P3700 and Optane as shuffle respectively.
* TeraSort: 100 GB
* WordCount: 100 GB


### 4. Performance Measurement

* End-to-end execution time
* CPU utilization
* Disk bandwidth, latency
* Network IO

Use Optane as HDFS Disk
-----------------

### 1. Purpose

* Optane serves as HDFS disks in datanodes
* Compare read/write performance of the workloads running on HDFS with HDD and P3700

### 2. Test Environment

* Cluster： 1 Namenode + 2 Datanodes
* HDFS Disk： 1 HDD+1 P3700 + 1 Optane on each datanode
* Hadoop version: trunk (3.0.0-beta1-SNAPSHOT)

### 3. Workload

For each test case, three experiments will be done with data on HDD, P3700 and Optane respectively. The data set size is 100 GB.

* TestDFSIO write, read
* Teragen, Terasort, Teraread, Teravalidate
* Wordcount

### 4. Performance Measurement

* End-to-end execution time
* CPU utilization
* Disk bandwidth, latency
* Network IO