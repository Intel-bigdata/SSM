# Disaster Recovery Performance Test on Large Namespace
Intuitively, storing large amount of records/data in database may lead to performance degrade. In our cases, large namespace (large amount of files on HDFS) may lead to performance degrade of SSM. Because SSM fetches all namespace from namenode and stores them in database (default is mysql). In this test, we want to evalute this issue on SSM Data Sync.

## Objectives
1. Evaluting SSM Data Sync's performance on large namespace.
2. Find and solve bottlenecks in SSM.
3. Find and solve bottlenecks in Data Sync module.

Baseline: Distcp

## Metrics and Analysis
### Environment
**10 Nodes are required:**

1. 8 nodes for 2 HDFS Clusters (each contains 1 namenode and 3 datanodes)
2. 1 node for SSM Server. SSM Agents are deployed on datanode of Primary HDFS.
3. 1 node for mysql.

Note that if there are less than 10 nodes available, we can merge mysql and SSM Server into 1 node (total 9 nodes). Also, we can remove one HDFS cluster, and copy among different dirs (5-6 nodes).

**Hardware/Software requirement**

**Hardware requriment:**
1. Namenode: at least 20GB memory and 15GB disk space are required. Please change the default heap size in `hadoop-env.sh`. Please change the location of `name` and `checkpoint` in `hdfs-site.xml` (Each `fsimage`/checkpoint will be at least 5GB).
2. Datanode: at least 2GB memory is requred by Datanode.
3. Mysql: at least 20GB memory and 30GB disk space are required. Please change mysql data dir to SSD in `my.cnf`, and tune mysql with [mysqltuner](http://mysqltuner.pl/).

**Software requirement:**
1. Hadoop (Hadoop-2.7.3/CDH-5.12.X or higher)
2. Mysql (5.6.X or higher)
3. SSM (1.4.X or higher)



### Common Variables and Setting
Namespace size: 0M, 25M, 50M, 75M and 100M
Task Concurrency: 30, 60, 90

### Metric to Collect
System Metrics: 
1. Total Running Time
2. Trigger Time for each increamental change
3. Average Running Time for each task
4. CPU usage
5. Memory usage
6. Disk I/O

1-3 can be collected from logs and command line. 4-6 can be collected by [PAT](https://github.com/intel-hadoop/PAT)

## Test Cases

### Case 1: Single File ASync

### Case 2: Batch ASync
