SSD测试计划
=========================


第一阶段：Optane测试 （WW31 - WW32）
------------


### 测试环境

#### 节点： 1 Namenode + 2 Datanode
#### 磁盘： 每个节点挂载 1 HDD+1 P3700 + 1 Optane
#### Hadoop version: trunk (3.0.0-beta1-SNAPSHOT)

### 测试指标

* End-to-end execution time
* Disk bandwith, disk latency
* CPU utilization
* Network IO

### Workload
* HDFS测试：
每项测试均对HDD、P3700和Optane分别测试得到三组对比值，数据集大小均为**100GB**。**Shortcircuit选项打开**。

1. TestDFSIO write, read
2. Teragen, Terasort, Teraread, Teravalidate
3. Wordcount

* Shuffle测试： 
测试SSD作为Yarn shuffle的性能，HDFS仅挂载一块HDD，Shuffle分别使用一块HDD、一块P3700以及一块Optane测试进行比较，Workload与HDFS测试一致（待定）。


第二阶段：
------------




