Optane Benchmark As Cache
===============

Test Environment
----------------

* Nodes: 1 NameNode + 2 DataNode
* Disks: 5 HDDs on each DataNode
* Memory: 375 GB DRAM + 375 GB Optane
* Hadoop version: 3.0.0-beta1-SNAPSHOT
* Spark version: 2.1

Benchmarks 
-------------------------

### 1. Micro-benchmark
Use ErasureCodeBenchmarkThroughput tool to measure the reading performance. Each test case will run in two modes: sequential read and random read.

### 2. End-to-end Benchmark
Use Spark Terasort to test the end-to-end performance.

Test Result
-------------------------

### 1. Micro-benchmark
![Throughput comparison][1]
* **Sequential read: cached situation offers 175% higher throughput than uncached situation.**
* **Random read: cached situation offers 226% higher throughput than uncached situation.**
* Cached situation has lower performance degrade when running random read vs. sequential read: the throughput is 2.9% down in cached situation, and 17.9% down in uncached situation.

Terasort
===============



  [1]: ./images/1502176966922.jpg


  