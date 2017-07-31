PCIe SSD在HDFS中的性能测试报告
============

测试环境
-------------

1. 集群配置
为简化测试环境，使用两台物理机，一个NameNode和一个DataNode，DataNode仅挂载一块磁盘，HDFS备份数为1。

2. Hadoop版本
Hadoop 3.0.0-beta1-SNAPSHOT

3. 网络
网卡带宽为20000Mb/s

4. 磁盘
分别对挂载SATA HDD和PCIe SSD的情况进行对比测试
SATA HDD: Seagate ST2000NM0011
PCIe SSD: Intel P3700

测试方法
-------------

1. 测试工具
使用ErasureCodeBenchmarkThroughput工具进行测试，这是Hadoop 3.0中自带的一个工具，用来测试HDFS数据的读写吞吐量，该工具同时支持EC方式和传统多备份方式存储的数据。

2. 数据集
数据集大小为**100GB**，测试单备份下写入和读取的吞吐量。**Shortcircuit选项关闭**。

3. 测试指标
ErasureCodeBenchmarkThroughput能够输出读写数据的吞吐量（单位MB/s），直接用此项输出作为衡量指标。同时也查看了DataNode clienttrace的log进行比对。

测试结果
-------------

1． 工具输出结果
下表为ErasureCodeBenchmarkThroughput的输出结果，分别测试了100GB数据的写入和读取。

|     |   SATA HDD (MB/s)  |   PCIe SSD (MB/s)  |
| --- | --- | --- |
|  写入   |  113.64   |  396.83   |
|  读取   |  140.25   |  2173.91   |

![enter description here][1]
		
2．LOG输出结果
以下截取了clienttrace的部分LOG数据（附件中包含了完整的LOG\

(1) HDD

* 写入：
2017-07-27 17:58:39,722 INFO org.apache.hadoop.hdfs.server.datanode.DataNode.clienttrace: src: /192.168.50.14:47042, dest: /192.168.50.14:9866, bytes: **671088640**, op: HDFS_WRITE, cliID: DFSClient_NONMAPREDUCE_-196230956_1, offset: 0, srvID: 7ab73c7d-a782-4388-bdf6-0b7b79b13ca9, blockid: BP-1486068274-192.168.50.7-1501068641236:blk_1073742048_1224, duration(ns): **5102260263**
* 读取：
2017-07-27 18:20:35,726 DEBUG org.apache.hadoop.hdfs.server.datanode.DataNode.clienttrace: src: /192.168.50.14:9866, dest: /192.168.50.14:47358, bytes: **676331520**, op: HDFS_READ, cliID: DFSClient_NONMAPREDUCE_-2091751233_1, offset: 0, srvID: 7ab73c7d-a782-4388-bdf6-0b7b79b13ca9, blockid: BP-1486068274-192.168.50.7-1501068641236:blk_1073742048_1224, duration(ns): **4562161072**

(2) SSD

* 写入：
2017-07-27 18:44:03,843 INFO org.apache.hadoop.hdfs.server.datanode.DataNode.clienttrace: src: /192.168.50.14:47398, dest: /192.168.50.14:9866, bytes: **671088640**, op: HDFS_WRITE, cliID: DFSClient_NONMAPREDUCE_1550518481_1, offset: 0, srvID: 7ab73c7d-a782-4388-bdf6-0b7b79b13ca9, blockid: BP-1486068274-192.168.50.7-1501068641236:blk_1073742205_1381, duration(ns): **1659875242**
* 读取：
2017-07-27 18:51:04,923 DEBUG org.apache.hadoop.hdfs.server.datanode.DataNode.clienttrace: src: /192.168.50.14:9866, dest: /192.168.50.14:47714, bytes: **676331520**, op: HDFS_READ, cliID: DFSClient_NONMAPREDUCE_827708685_1, offset: 0, srvID: 7ab73c7d-a782-4388-bdf6-0b7b79b13ca9, blockid: BP-1486068274-192.168.50.7-1501068641236:blk_1073742205_1381, duration(ns): **311877251**

通过以上LOG的粗体部分计算得到的吞吐量与ErasureCodeBenchmarkThroughput工具输出的值基本一致。


  [1]: ./images/1501488975399.jpg