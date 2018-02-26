# Disaster Recovery Performance Test on Large Namespace
Intuitively, storing large amount of records/data in database may lead to performance degrade. In our cases, large namespace (large amount of files on HDFS) may lead to performance degrade of SSM. Because SSM fetches all namespace from namenode and stores them in database (default is mysql). In this test, we want to evalute this issue on SSM Data Sync.

## Objectives
1. Evaluting SSM Data Sync's performance on large namespace.
2. Find and solve bottlenecks in SSM.
3. Find and solve bottlenecks in Data Sync module.

Baseline: Distcp

## Metrics and Analysis
### Environment
10 Nodes are required:
1. 2 HDFS Clusters (Hadoop-2.7.3, each contains 1 namenode and 3 datanodes)
2. 1 node for SSM Server (1.4.X or later). SSM Agents are deployed on datanode of Primary HDFS.
3. 1 node for mysql (5.6.X or later)

### Common Variables and Setting
Namespace size: 0M, 25M, 50M, 75M and 100M
Task Concurrency: 30, 60, 90

### Metric to Collect
System Metrics: CPU usage, Memory usage and I/O
Running Time: 


## Test Cases

### Case 1: Single File ASync

### Case 2: Batch ASync
