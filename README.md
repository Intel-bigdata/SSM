
HDFS Smart Storage Management [![Build Status](https://travis-ci.org/Intel-bigdata/SSM.svg?branch=trunk)](https://travis-ci.org/Intel-bigdata/SSM?branch=trunk)
=========================

**HDFS-SSM** is the major portion of the overall [Smart Data Management Initiative](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/overall-initiative.md).

Big data is putting increasing pressure on HDFS storage in recent years with varous of workloads and demanding performance. The latest storage devices (Optane Memory, Optane SSD, NVMe SSD, etc.) can be used to improve the storage performance. Meanwhile HDFS provides all kinds of nice methodologies like HDFS Cache, Heterogeneous Storage Management (HSM) and Erasure Coding (EC), but it remains a big challenge for users to make full utilization of these high-performance storage devices and HDFS storage options in a dynamic environment.

To overcome the challenge, we introduced a comprehensive end-to-end solution, aka Smart Storage Management (SSM) in Apache Hadoop. HDFS operation data and system state information are collected, based on the collected metrics SSM can automatically make sophisticated usage of these methodologies to optimize HDFS storage efficiency.

High Level Goals
------------
### 1. Enhancement for HDFS-HSM and HDFS-Cache
**Automatically** and **smartly** adjusting storage policies and options in favor of data temperature. Already released.
### 2. Support block level erasure coding
Similar to the old [HDFS-RAID](https://wiki.apache.org/hadoop/HDFS-RAID), not only for **Hadoop 3.x**, but also **Hadoop 2.x**. Ref. the [block level erasure coding design](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/block-level-ec.md).
### 3. Small files support and compaction
Optimizing NameNode to support even larger namespace, eliminating the inodes of small files from memory. Support both write and read. Ref. the [HDFS small files support design](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/small-file-solution.md).
### 4. Cluster Data Copy and Disaster Recovery
Supporting transparent fail-over for applications. Here is the [HDFS disaster recovery design](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/disaster-recovery.md) document. The 1st stage already released. 
### 5. Transparent HDFS Data Compression
Supporting transparent HDFS data compression, note it's not Hadoop compression, which needs to be explicitly called by applications and frameworks like MR. Under prototyping and design coming soon. 

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
SSM overall as follows. Ref. [SSM design](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/hdfs-ssm-design.md) for details. Note some of the contents need to be updated according to the lastest implementation.

<img src="https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/ssm-overall.png" />

How SSM server and agents collaborate to serve for one specific service, like move HDFS file blocks among storage tiers, copy files to back up cluster or block erasure coding? Please ref. below picture.

<img src="https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/ssm-overall-2.png" />

The following picture depicts SSM system behaviours.

<img src="https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/ssm-lifecycle.png" />

Below figure illustrates how to position SSM in big data ecosystem.
<img src="https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/high-level-architecture.png" />

Development Phases
------------
HDFS-SSM development is separated into 3 major phases. Currently the Phase 1 work is approaching completion.

**Phase 1.** Implement SSM framework and the fundamental infrastructure:
* Event and metrics collection from HDFS cluster;
* Rule DSL to support high level customization and usage;
* Support richful smart actions to adjust storage policies and options, enhancing HDFS-HSM and HDFS-Cache;
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
When the files got very hot, they can be moved from fast storage into cache memory to achieve the best read performance. The following shows the example of moving data to memory cache if the data has been read over 3 times during the last 5 minutes

![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/cache-case.png)

### 2. Move hot data to fast storage
![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/ssd-case.png)

Without SSM, data may always be readed from HDD. With SSM, optimizations can be made through rules. As showned in the figure above, data can be moved to faster SSD to achieve better performance.

### 3. Archive cold data
Files are less likely to be read during the ending of lifecycle, so it’s better to move these cold files into lower performance storage to decrease the cost of data storage. The following shows the example of archiving data that has not been read over 1 times during the last 90 days.

![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/archive-case.png)

Admin Doc
------------
Cluster administrator takes the role of SSM rule management. A set of APIs is exposed to help administrator manage rule. This set of APIs includes create, delete, list, enable and disable SSM rule. Hadoop admin privilege is required for access the APIs. For detailed information, please refer to [Admin Guide](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/admin-user-guide.md).

User Doc
------------
SSM will provide a SmartDFSClient that includes both original HDFS DFSClient APIs and new SSM APIs. Applications can use this SmartDFSClient to benefit from the provided SSM facilities. New SSM APIs include cache file and archive file etc. More APIs will be added later. For detailed information, please refer to [User Guide](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/client-user-guide.md).

How to Build
------------
Currently SSM supports 2 hadoop major versions, hadoop 2.7.X and CDH 5.10.1, which is based on 2.6.0.
So you can build SSM with command

```
mvn package -P dist,hadoop-2.7
```

or

```
mvn package -P dist,hadoop-cdh-2.6
```

Then there will be a `smart-data-${version}.tar.gz` built out under `smart-dist/target`.

Third-party dependencies
------------------------
SSM uses SQL database as its backend metastore. The following two databases were tested and recommended for using in production environment:
**MySQL** : User have to deploy MySQL service manually.
**TiDB** : TiDB instance can be embedded into SSM for convenience of deployment. Independent TiDB deployment is also supported.
Please ref. [SSM Deployment Guide](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/ssm-deployment-guide.md) for more details.

Developers
------------
- Wei, Zhou
- Huafeng, Wang
- Qiyuan, Gong
- Yi, Chen
- Kai, Zheng
- Jie, Tao
- Shunyang, Yao
- Zhiqiang, Zhang
- Feilong, He
- Frank, Zeng
- Tianlun, Zhang
- Yuxuan, Pan
- Jiajia, Li
- Zhen, Du
- Yao, Chen
- Chen, Feng
- Yanping, You
- Tianlong, Xu
- Anyang, Wang
- Conghui, Li
- Weijun, Pang

How to Contribute
------------
We welcome your feedback and contributions. Please feel free to fire issues or push PRs, we'll respond soon. Note the project is evolving very fast. 

Acknowledgement
------------
This originates from and bases on the discussions occurred in Apache Hadoop JIRA [HDFS-7343](https://issues.apache.org/jira/browse/HDFS-7343). It not only thanks to all the team members of this project, but also thanks a lot to all the idea and feedback contributors.
