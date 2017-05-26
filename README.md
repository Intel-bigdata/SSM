
HDFS-SSM - HDFS Smart Storage Management [![Build Status](https://travis-ci.org/Intel-bigdata/SSM.svg?branch=ssm)](https://travis-ci.org/Intel-bigdata/SSM?branch=ssm)
=========================

This is the major portion of the overall [Smart Data Management Initiative](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/overall-initiative.md).

Big data has put increasing pressure on HDFS storage in recent years. The latest storage devices (3D XPoint(R) SSD, NVMe SSD, etc.) can be used to improve the storage performance. HDFS provides methodologies like HDFS Cache, Heterogeneous Storage Management and Erasure Coding to provide such support, but it still remains a big challenge for HDFS to make full utilization of these high-performance storage devices in a dynamic environment.

To overcome the challenge, we introduced in a comprehensive end-to-end solution, aka Smart Storage Management (SSM) in Apache Hadoop. HDFS operation data and system state information are collected from the cluster, based on the metrics collected SSM can automatically make sophisticated usage of these methodologies to optimize HDFS storage efficiency.

Goal
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

Architecture
------------
![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/architecture.png)

SSM polls metrics from NameNode. These metrics are analyzed by SSM as specified by rules, and if conditions of some rule are fulfilled then it will execute the corresponding actions. 

SSM uses SQL database to maintain data polled as well as other internal data. Some of the data are required for SSM states recovery, so SSM will checkpoint them into HDFS in time.

### Compatibility
SSM is being developed based on Hadoop trunk, but it can be ported/integrated to lower Hadoop versions or even custom private Hadoop versions easily.

### HA Support
Hadoop HA is supported by SSM. There can be some metrics data loss (file.accessCount) for a short time interval when NN deactivate happens. SSM can stand this data loss and won't cause disaster.

Desgin
------------
SSM consists of 5 chief components illustrated in the following figure:
![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/design.png)

* StatesManager
	* Collect metrics and events from NameNode
	* Maintain data and forward events 
	
* RuleManager
	* Manage rules
	* Schedule and execute rules
	
* CacheManager
	* Schedule the execution of cache related actions
	
* StorageManager
	* Schedule the execution of storage related actions
	
* ActionExecutor
	* Execute actions generated
	
## Rules
A rule is an interface between user and SSM, through which the user tells SSM how to function. A rule defines all the things for SSM to work: at what time, analysis what kind of metrics and conditions, and what actions should be taken when the conditions are true. By writing rules, a user can easily manage their cluster and adjust its behavior for certain purposes.
![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/usage.png)

### Rule Syntax

![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/rule-syntax.png)

### SSM Meta Store
SSM uses a SQL database as MetaStore to maintain data meta infos internally. Core tables in SSM:

![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/core-tables.png)

#### Access Count Collection
Below is to illustrate how to collect file access counts. As shown in the following chart:

![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/access-count-tables.png)

1. SSM polls accessCount data from NN to get file access count info generated in the time interval (for example, 5s).
2. Create a table to store the info and insert the table name into table access_count_tables.
3. Then file access count of last time interval can be calculated by accumulating data in tables that their start time and end time falls in the interval.
4. To control the total amount of data, second-level of accessCount tables will be aggregated into minute-level, hour-level, day-level, month-level and year-level. The longer the time from now, the larger the granularity for aggregation. More accurate data kept for near now than long ago.
