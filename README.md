
HDFS Smart Storage Management [![Build Status](https://travis-ci.org/Intel-bigdata/SSM.svg?branch=trunk)](https://travis-ci.org/Intel-bigdata/SSM?branch=trunk)
=========================

**HDFS-SSM** is the major portion of the overall [Smart Data Management Initiative](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/overall-initiative.md).

In big data field, HDFS storage has been facing increasing pressure due to various workloads and demanding performance requirements in recent years. The latest storage devices (Optane Memory, Optane SSD, NVMe SSD, etc.) can be used to improve the storage performance. Meanwhile HDFS provides all kinds of nice methodologies like HDFS Cache, Heterogeneous Storage Management (HSM) and Erasure Coding (EC), but it is a big challenge for users to make full utilization of these high-performance storage devices and HDFS storage options in a dynamic environment.

To overcome the challenge, we have introduced a comprehensive end-to-end solution, aka Smart Storage Management (SSM) in Apache Hadoop. HDFS operation data and system state information are collected, and based on the collected metrics, SSM can automatically make sophisticated usage of these methodologies to optimize HDFS storage efficiency.

High Level Goals
------------
### 1. Enhancement for HDFS-HSM and HDFS-Cache
**Automatically** and **smartly** adjusting storage policies and options according to data temperature. Already released.
### 2. Support block level erasure coding
Similar to the old [HDFS-RAID](https://wiki.apache.org/hadoop/HDFS-RAID), not only for **Hadoop 3.x**, but also for **Hadoop 2.x**. Ref. the [block level erasure coding design](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/block-level-ec.md).
### 3. Small files support and compaction
Optimizing NameNode to support even larger namespace, and eliminating the number of inodes for small files from memory. Supporting both write and read. Ref. the [HDFS small files support design](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/small-file-solution.md).
### 4. Cluster Data Copy and Disaster Recovery
Supporting transparent fail-over for applications. Here is the [HDFS disaster recovery design](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/disaster-recovery.md) document. The 1st stage already released.
### 5. Transparent HDFS Data Compression
Supporting transparent HDFS data compression. Note that it is not Hadoop compression, which needs to be explicitly called by applications and frameworks like MR.

High Level Considerations
------------
1. Support Hadoop 3.x and Hadoop 2.x;
2. The whole work and framework builds on top of HDFS, avoiding modifications when possible;
3. Compatible HDFS client APIs to support existing applications and computation frameworks;
4. Provide additional client APIs to allow new applications to benefit from SSM nice facilities;
5. Support high availability and reliability, trying to reuse existing infrastructures in a deployment;
6. Security is ensured, particularly when Hadoop security is enabled.

Architecture
------------
SSM architecture is shown as follows. Ref. [SSM design](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/hdfs-ssm-design.md) for details. Note that some of the contents need to be updated according to the latest implementation.

<img src="https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/ssm-overall.png" />

How SSM server and agents collaborate to perform one specific service? The below picture illustrates how SSM server and agents collaborate to perform some services.

<img src="https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/ssm-overall-2.png" />

The following picture depicts SSM system behaviours.

<img src="https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/ssm-lifecycle.png" />

Below figure illustrates how to position SSM in big data ecosystem.
<img src="https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/high-level-architecture.png" />

Development Phases
------------
HDFS-SSM development is separated into 3 major phases. Currently the Phase 2 work is approaching completion.

**Phase 1.** Implement SSM framework and the fundamental infrastructure:
* Event and metrics collection from HDFS cluster;
* Rule DSL to support high level customization and usage;
* Support various smart actions to adjust storage policies and options, enhancing HDFS-HSM and HDFS-Cache;
* Client API, Admin API following Hadoop RPC and REST channels;
* Basic web UI support.

**Phase 2.** Refine SSM framework and support user solutions:
* Small files support and compaction;
* Cluster disaster recovery;
* Support block level erasure coding;
* Transparent HDFS data compression;
* To support the new desired actions, enhance the SSM framework and infrastructure.

**Phase 3.** Optimize further for computing frameworks and workloads benefiting from SSM offerings and facilities:
* Hive on SSM;
* HBase on SSM;
* Spark on SSM;
* Deep Learning on SSM.

Phase I -- Use Cases 
------------
### 1. Cache most hot data
When the files get very hot, they can be moved from fast storage into cache memory to achieve the best read performance. The following shows the example of moving data to memory cache if the data has been read over 3 times during the last 5 minutes

![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/cache-case.png)

### 2. Move hot data to fast storage
![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/ssd-case.png)

Without SSM, data may always be read from HDD. With SSM, optimizations can be made through rules. As shown in the figure above, data can be moved to faster SSD to achieve better performance.

### 3. Archive cold data
Files are less likely to be read during the ending of lifecycle, so it is better to move these cold files into lower performance storage to decrease the cost of data storage. The following shows the example of archiving the data that has not been read during the last 90 days.

![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/archive-case.png)

Admin Doc
------------
Cluster administrator takes the role of SSM rule management. A set of APIs is exposed to help administrator manage rule. This set of APIs includes create, delete, list, enable and disable SSM rule. Hadoop admin privilege is required to access the APIs. For detailed information, please refer to [Admin Guide](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/admin-user-guide.md).

User Doc
------------
SSM provides a SmartDFSClient that includes both original HDFS DFSClient APIs and new SSM APIs. Applications can use this SmartDFSClient to benefit from the provided SSM facilities. New SSM APIs include cache file and archive file etc. More APIs will be added later. For detailed information, please refer to [User Guide](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/client-user-guide.md).

How to Build
------------
Currently SSM supports CDH 5.10.1 (which is based on hadoop-2.6.0), hadoop-2.7.X and hadoop-3.1.X.
So you can build SSM with the following commands accordingly.

```
mvn package -D skipTests -P dist,web,hadoop-cdh-2.6
```

```
mvn package -D skipTests -P dist,web,hadoop-2.7
```

```
mvn package -D skipTests -P dist,web,hadoop-3.1
```

Then a package named as `smart-data-${version}.tar.gz` will be generated under `smart-dist/target`.

For other versions of hadoop, we had no complete tests on them. But if SSM's dfs client version, determined by above build option, is compatible with a version of hadoop, there will be no issue.
Our incomplete tests showed that SSM built for hadoop-3.1.0 can also support hadoop-3.0.0-cdh6.0.1.

Quick Start
-----------
Please refer to [SSM deployment guide](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/ssm-deployment-guide.md).

Third-party dependencies
------------------------
SSM uses SQL database as its backend metastore. MySQL is used in our multiple tests and it is recommended in production environment.

**MySQL** : User should deploy MySQL service in advance.

How to Contribute
------------
We welcome your feedback and contributions. Please feel free to submit issues or PRs, and we'll respond soon. Note that the project is evolving very fast.

Acknowledgement
------------
This work derives from the discussions in Apache Hadoop JIRA [HDFS-7343](https://issues.apache.org/jira/browse/HDFS-7343). We want to thank not only all the team members of this project, but also all the contributors for their valuable ideas and feedback.

Important Notice and Contact Information
-------
a) SSM is developed and open sourced by Intel Data Analytics Technology team. Please reach us via `feilong.he{at}intel.com` or `jian.zhang{at}intel.com` if you have any question or need help.

b) To help develop SSM further, please become an active member of the community and consider giving back by making contributions.


**For any security concern, please visit https://01.org/security.**