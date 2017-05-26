
HDFS Smart Storage Management [![Build Status](https://travis-ci.org/Intel-bigdata/SSM.svg?branch=ssm)](https://travis-ci.org/Intel-bigdata/SSM?branch=ssm)
=========================

**HDFS-SSM** is the major portion of the overall [Smart Data Management Initiative](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/overall-initiative.md).

Big data has put increasing pressure on HDFS storage in recent years. The latest storage devices (3D XPoint(R) SSD, NVMe SSD, etc.) can be used to improve the storage performance. HDFS provides methodologies like HDFS Cache, Heterogeneous Storage Management and Erasure Coding to provide such support, but it still remains a big challenge for HDFS to make full utilization of these high-performance storage devices in a dynamic environment.

To overcome the challenge, we introduced in a comprehensive end-to-end solution, aka Smart Storage Management (SSM) in Apache Hadoop. HDFS operation data and system state information are collected from the cluster, based on the metrics collected SSM can automatically make sophisticated usage of these methodologies to optimize HDFS storage efficiency.

High Level Goals
------------
* Enhancement for HDFS-HSM and HDFS-cache;
* Support block EC, similar to HDFS-RAID;
* Small files compaction and optimization;
* Cluster Disaster Recovery and transparent fail-over

Development Phases
------------
The SSM project is separated into two phases:

**Phase 1.** Implement a rule-based automatic engine that can execute user-defined rules to do automation stuffs. It provides a unified interface (through rules) to manage and tune HDFS cluster.

**Phase 2.** Implement a rule-generator that can be aware of all these factors, and can automatically generate comprehensive rules. It’s the advanced part and makes SSM works smartly.

The project is on Phase 1 now, it includes the following general subtasks:
* Define metrics to collect from HDFS
* Define rule DSL regarding syntax and build-in variables used for rules.
* Implement rule engine and different actions
* Optimize facilities to cooperate with SSM
* Implement UI to make SSM easy to use 

Use Cases
------------
### Optimizations when data becoming hot
![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/hot-cases.png)

Without SSM, data will always be readed from HDD (a.). With SSM, optimizaitons can be made through rules. As showned in the figure above, data can be moved to faster storage (b.) or cached in memory (c.) to achive better performance.

### Archive cold data
Files are less likely to be read during the ending of lifecycle, so it’s better to move these cold files into lower performance storage to decrease the cost of data storage. The following rule shows the example of archiving data that has not been read over 3 times during the last 30 days.

![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/archive-rule.png)

Admin Doc
------------

User Doc
------------

Acknowlegement
------------
This originates from and bases on the discussions occured in Apache Hadoop JIRA [HDFS-7343](https://issues.apache.org/jira/browse/HDFS-7343). It not only thanks to all the team members of this project, but also thanks a lot to all the idea and feedback contributors. Particularly the following folks from various parties across Hadoop community and big data industry:
* Andrew Wang;
* Jing Zhao;
* Eddy Xu;
* Anu Engineer;
* Andrew Purtell;
* Chris Douglas;
* Weihua Jiang
