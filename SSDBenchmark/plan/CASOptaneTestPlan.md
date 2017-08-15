Optane + CAS Test Plan
==============

Compare CAS+ Optane with HDFS Lazy Persist
-----------------

### 1. Purpose
•	Verify that Optane + CAS can be used in HDFS Low Latency Write
•	Compare the performance of Optane + CAS with RAMDISK
•	Test the HDFS writing performance

### 2. Test Environment
* Cluster： 1 Namenode + 2 Datanodes
* Disk： 5 HDDs on each datanode
* Memory : 375 GB
* Optane : 375 GB
* Hadoop version: trunk (3.0.0-beta1-SNAPSHOT)

### 3.WorkLoad
•	Use TestDFSIO tool to measure the performance. 
•	1 replica
•	Test cases:

| Case Name | Storage | Dataset | Comment |
| --------- | ------- | ------- | ------- |
| 100G_RAM_HDD | 1RAMDISK 5HDDs | 100G | Using HDFS lazy persist mode |
| 100G_Optane_CAS_HDD | 1Optane 5HDDs | 100G | Using CAS write back mode |
| 100G_SSD | 1SSD | 100G | Store all the files in SATA SSD |
| 100G_HDD | 5HDDs | 100G | Store all the files in HDD |
| 200G_RAM_HDD | 1RAMDISK 5HDDs | 200G | Using HDFS lazy persist mode |
| 200G_Optane_CAS_HDD | 1Optane 5HDDs | 200G | Using CAS write back mode |
| 200G_SSD | 1SSD | 200G | Store all the files in SATA SSD |
| 200G_HDD | 5HDDs | 200G | Store all the files in HDD |
| 300G_RAM_HDD | 1RAMDISK 5HDDs | 300G | Using HDFS lazy persist mode |
| 300G_Optane_CAS_HDD | 1Optane 5HDDs | 300G | Using CAS write back mode |
| 300G_SSD | 1SSD | 300G | Store all the files in SATA SSD |
| 300G_HDD | 5HDDs | 300G | Store all the files in HDD |

### 4. Performance Measurement
Throughput of the writing

 
HBASE WAL
-----------------

### 1. Purpose
•	HBASE writhing operation can be accelerated by WAL using Optane + CAS 

### 2. Test Environment
* Cluster： 1 Namenode + 2 Datanodes + 1 Master + 2 RegionServer
* Disk： 5 HDDs on each datanode
* Memory : 375 GB
* Optane : 375 GB
* Software stack version and configuration:

| Software | Details |
| -------  | ------- |
| Hadoop	trunk | (3.0.0-beta1-SNAPSHOT) |
| HBase | 1.2.6 |
| Zookeeper |	3.4.6 |
| YCSB | 0.12.0 |
| JDK |	JAVA8 |
| JVM Heap | NameNode:32GB DataNode:4GB HMaster:4GB RegionServer:64GB|
| GC | G1GC|

### 3.WorkLoad
•	YCSB (Yahoo! Cloud Serving Benchmark, a widely used open source framework for evaluating the performance of data-serving systems) is used as the test workload.
•	Test cases:

| Case Name | Storage | Dataset | Comment |
| --------- | ------- | ------- | ------- |
| 100G_RAM | 1RAMDISK | 100G | Store all files in RAMDISK. |
| 100G_CAS_Optane | 1Optane | 100G | Store all files in Optane. |
| 100G_RAM_HDD | 1RAMDISK 5HDDs | 100G | Store WAL in RAMDISK, other files are stored in HDD|
| 100G_Optane_CAS_HDD | 1Optane 5HDDs | 100G | Store WAL in Optane, other files are stored in HDD |
| 100G_SSD | 1SSD | 100G | Store all the files in SATA SSD |
| 100G_HDD | 5HDDs | 100G | Store all the files in HDD |
| 200G_RAM | 1RAMDISK | 200G | Store all files in RAMDISK. |
| 200G_CAS_Optane | 1Optane | 200G | Store all files in Optane. |
| 200G_RAM_HDD | 1RAMDISK 5HDDs | 200G | Store WAL in RAMDISK, other files are stored in HDD |
| 200G_Optane_CAS_HDD | 1Optane 5HDDs | 200G | Store WAL in Optane, other files are stored in HDD |
| 200G_SSD | 1SSD | 200G | Store all the files in SATA SSD |
| 200G_HDD | 5HDDs | 200G | Store all the files in HDD |
| 300G_RAM | 1RAMDISK | 300G | Store all files in RAMDISK. |
| 300G_CAS_Optane | 1Optane | 300G | Store all files in Optane. |
| 300G_RAM_HDD | 1RAMDISK 5HDDs | 300G | Store WAL in RAMDISK, other files are stored in HDD |
| 300G_Optane_CAS_HDD | 1Optane 5HDDs | 300G | Store WAL in Optane, other files are stored in HDD |
| 300G_SSD | 1SSD | 300G | Store all the files in SATA SSD |
| 300G_HDD | 5HDDs | 300G | Store all the files in HDD |

### 4. Performance Measurement
•	End-to-end execution time
•	Throughput of the writing workload
•	Disk bandwidths


 


