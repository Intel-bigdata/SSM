Optane Shuffle Test Plan
=========================

Purpose
-----------------

* Optane serves as shuffle disk for Hadoop 
* Compare with HDD and P3700 as shuffle


Test Environment
-----------------

* Cluster： 1 Namenode + 2 Datanodes
* HDFS Disk： 5 HDDs on each datanode
* Memory : 100 GB
* Shuffle Disk : 1 HDD / 1 P3700 / 1 Optane
* Hadoop version: trunk (3.0.0-beta1-SNAPSHOT)

Workload
-----------------

For each workload, three experiments will be done using HDD, P3700 and Optane as shuffle respectively.
* TeraSort: 200 GB
* WordCount: 200 GB


Performance Measurement
-----------------

* End-to-end execution time
* CPU utilization
* Disk bandwidth, latency
* Network IO