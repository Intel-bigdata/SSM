
HDFS Smart Storage Management [![Build Status](https://travis-ci.org/Intel-bigdata/SSM.svg?branch=trunk)](https://travis-ci.org/Intel-bigdata/SSM?branch=trunk)
=========================

**HDFS-SSM** is the major portion of the overall [Smart Data Management Initiative](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/overall-initiative.md).

Big data has put increasing pressure on HDFS storage in recent years. The latest storage devices (3D XPoint(R) SSD, NVMe SSD, etc.) can be used to improve the storage performance. HDFS provides methodologies like HDFS Cache, Heterogeneous Storage Management and Erasure Coding to provide such support, but it still remains a big challenge for HDFS to make full utilization of these high-performance storage devices in a dynamic environment.

To overcome the challenge, we introduced in a comprehensive end-to-end solution, aka Smart Storage Management (SSM) in Apache Hadoop. HDFS operation data and system state information are collected from the cluster, based on the metrics collected SSM can automatically make sophisticated usage of these methodologies to optimize HDFS storage efficiency.

High Level Goals
------------
### 1. Enhancement for HDFS-HSM and HDFS-Cache
**Automatically** and **smartly** adjusting storage policies and options.
### 2. Support block level erasure coding
Similar to the old [HDFS-RAID](https://wiki.apache.org/hadoop/HDFS-RAID), not only for **Hadoop 3.x**, but also **Hadoop 2.x**.
### 3. Small files support and compaction
Optimizing NameNode to support even larger namespace, eliminating the inodes of small files from memory.
### 4. Cluster Disaster Recovery
Supporting transparent fail-over for applications. Here is the [High Level Design](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/disaster-recovery.md) document. 

High Level Considerations
------------
1. Supports Hadoop 3.x and Hadoop 2.x;
2. The whole work and framework builds on top of HDFS, avoiding modifications when possible;
3. Compatible HDFS client APIs to support existing applications, computation frameworks;
4. Provide addition client APIs to allow new applications to benefit from SSM nice facilities;
5. Support High Availability and reliability, trying to reuse existing infrastructures in a deployment when doing so;
6. Security is ensured, particularly when Hadoop security is enabled.

Architecture
------------
The following picture depicts SSM system behaviours.
<img src="https://github.com/Intel-bigdata/SSM/blob/trunk/docs/ssm-lifecycle.png" />

Below figure illustrates how to position SSM in big data ecosystem. Ref. [SSM architecture](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/hdfs-ssm-design.md) for details.
<img src="https://github.com/Intel-bigdata/SSM/blob/trunk/docs/high-level-architecture.png" />

Development Phases
------------
HDFS-SSM development is separated into 3 major phases. Currently the Phase 1 work is approaching completion.

**Phase 1.** Implement SSM framewwork and the fundamental infrustrature:
* Event and metrics collection from HDFS cluster;
* Rule DSL to support high level customization and usage;
* Support richful smart actions to adjust storage policies and options, enhancing HDFS-HSM and HDFS-Cache;
* Client API, Admin API following Hadoop RPC and REST channels;
* Basic web UI support.

**Phase 2.** Refine SSM framework and support user solutions:
* Small files support and compaction;
* Cluster disaster recovery;
* Support block level erasure coding;
* To support the new desired actions, enhance the SSM framework and infrastructure.

**Phase 3.** Optimize further for computing frameworks and workloads benefiting from SSM offerings and facilities:
* Hive on SSM;
* HBase on SSM;
* Spark on SSM;
* Deep Learning on SSM.

Phase I -- Use Cases 
------------
### 1. Cache most hot data
When the files got very hot, they can be moved from fast storage into cache memory to achieve the best read performance. The following shows the example of moving data to memory cache if the data has been read over 3 times during the last 5 minutes

![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/cache-case.png)

### 2. Move hot data to fast storage
![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/ssd-case.png)

Without SSM, data may always be readed from HDD. With SSM, optimizaitons can be made through rules. As showned in the figure above, data can be moved to faster SSD to achive better performance.

### 3. Archive cold data
Files are less likely to be read during the ending of lifecycle, so it’s better to move these cold files into lower performance storage to decrease the cost of data storage. The following shows the example of archiving data that has not been read over 1 times during the last 90 days.

![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/archive-case.png)

Admin Doc
------------
Cluster admininstrator takes the role of SSM rule management. A set of APIs is exposed to help administrator manage rule. This set of APIs includes create, delete, list, enable and disable SSM rule. Hadoop super user privilege is required for access the APIs. For detail information, please refer to [Admin Guide](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/admin-user-guide.md).

User Doc
------------
SSM will provide a SmartClient which includes both original HDFS DFSClient functions and new SSM user application APIs. Upper level application can use this SmartClient instead of original HDFS DFSClient. New SSM user application APIs include move file and archive file etc. More functions will be added later. System will execute the operation on half of application, with the privilege of the user who starts the application. For detail information, please refer to [User Guide](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/client-user-guide.md).

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
