SSD测试计划
=========================


第一阶段：Optane测试 （WW31 - WW32）
------------


----------


### 测试环境

#### 节点： 1 Namenode + 2 Datanode
#### 磁盘： 每个节点挂载 1 HDD+1 P3700 + 1 Optane
#### Hadoop: trunk (3.0.0-beta1-SNAPSHOT)


### Workload
* 每项测试均对HDD、P3700和Optane分别测试得到三组对比值，数据集大小均为100GB

1. TestDFSIO write, read
2. Teragen, Terasort, Teraread, Teravalidate
3. Wordcount

* Shuffle测试： 







