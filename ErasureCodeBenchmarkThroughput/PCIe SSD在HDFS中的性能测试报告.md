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

三、测试结果
1． 工具输出结果
下表为ErasureCodeBenchmarkThroughput的输出结果，分别测试了100GB数据的写入和读取。

|     |   SATA HDD (MB/s)  |   PCIe SSD (MB/s)  |
| --- | --- | --- |
|  写入   |  113.64   |  396.83   |
|  读取   |  140.25   |  2173.91   |

![enter description here][1]
		


  [1]: ./images/1501488975399.jpg