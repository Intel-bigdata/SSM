# Disaster Recovery Performance Test on Large Namespace
Intuitively, storing large amount of records/data in database may lead to performance degrade. In our cases, large namespace (large amount of files on HDFS) may lead to performance degrade of SSM. Because SSM fetches all namespace from namenode and stores them in database (default is mysql). In this test, we want to evalute this issue on SSM Data Sync.

## Objectives
1. Evaluting SSM Data Sync's performance on large namespace (from 10M~100M).
2. Find/solve bottlenecks in SSM.
3. Find/solve bottlenecks in Data Sync module.

**Baseline: Distcp**

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
3. Mysql: at least 20GB memory and 30GB disk space are required. Please change mysql data dir location to SSD in `my.cnf`, and tune mysql with [mysqltuner](http://mysqltuner.pl/). Otherwise, Mysql will become the main bottleneck of SSM.

**Software requirement:**

1. Hadoop (Hadoop-2.7.3/CDH-5.12.X or higher)
2. Mysql (5.6.X or higher)
3. SSM (1.3.2 or higher)


### Variables and Metics
All variables about HDFS, SSM and Distcp, bolded as default.

**Main Variables:**

1. Namespace size: 0M, 10M, 25M, 50M, 75M and 100M
2. Task Concurrency: 30, **60**, 90

**SSM Variables:**

1. Rule Check period: 100ms, **200ms**, 500ms, 1s
2. Cmdlet batch size: 100, **200**, 300
3. Data Sync Check period: **100ms**, 200ms
4. Data Sync diff batch size: 100, **200**, 300
5. Database: local mysql, **remote mysql** and TiDB

Note that lots of variables may effect SSM Data Sync's performance. I think we can fix some variables and focus on main variables.

### Metrics to Collect
**System Metrics:**

1. Total Running Time
2. Trigger Time for each increamental change
3. Average Running Time for each task
4. Scheduler Time
5. CPU usage
6. Memory usage
7. Disk I/O

1-3 can be collected from logs and command line. 4-6 can be collected by [PAT](https://github.com/intel-hadoop/PAT).

## Test Cases

### Case 1: Batch ASync
Copy **a batch of files** from primary HDFS cluster to sencondary HDFS cluster, and check the **total running time**, **scheduler time** and **system metrics** on these cases:

1. 10000 * 10KB
2. **10000 * 1MB**
3. 10000 * 10MB
4. 100 * 100MB
5. 100 * 200MB

Case 1-3 is less than block size, case 4-5 is larger than block size. Note that system cache should be cleaned before running test case.

**Baseline: DistCp**

### Case 2: Single File ASync
Copy **a single of file** from primary HDFS cluster to sencondary HDFS cluster, and check the **total running time**, **scheduler time** and **system metrics** on these cases:

1. 1MB
2. **100MB**
3. 1GB
4. 2GB
5. 5GB


**Baseline: DistCp**


## Trouble Shooting
