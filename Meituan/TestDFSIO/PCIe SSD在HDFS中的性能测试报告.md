PCIe SSD在HDFS中的性能测试报告
===========

测试环境
---------------

1. 集群配置
	为简化测试环境，使用两台物理机，一个NameNode和一个DataNode，DataNode仅挂载一块磁盘，HDFS备份数为1。

2. Hadoop版本
	Hadoop 2.7.1

3. 网络
	网卡带宽为20000Mb/s

4. 磁盘
	分别对挂载SATA HDD和PCIe SSD的情况进行对比测试
* SATA HDD: Seagate ST2000NM0011
* PCIe SSD: Intel P3700

测试方法
---------------

1. 测试工具
使用TestDFSIO进行测试。

2. 数据集
数据集大小为**100GB**，HDFS block size为**64MB**，测试单备份下写入和读取的吞吐量。**Shortcircuit选项关闭**。

3. 测试指标
TestDFSIO能够输出读写数据的吞吐量（单位MB/s），直接用此项输出作为衡量指标。同时也查看了DataNode clienttrace的log进行比对。

测试结果
---------------

1． 工具输出结果
下表为TestDFSIO的输出结果，分别测试了100GB数据的写入和读取。

|     |  写入   |  读取   |
| --- | --- | --- |
|  SATA HDD (MB/s)   |  105   |  134   |
|  P3700 SSD(MB/s)   |  421   |  1811   |

![enter description here][1]
 

2．LOG输出结果
下图为通过LOG输出计算得到的读写吞吐量（附件中包含了完整的LOG）。

 ![enter description here][2]
 
可以看出，与工具输出值的情况基本保持一致。


  [1]: ./images/1501499547668.jpg
  [2]: ./images/1501499595811.jpg