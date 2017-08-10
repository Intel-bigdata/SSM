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
Use Terasort to test the end-to-end performance.

Test Result
-------------------------

### 1. Micro-benchmark
![Micro benchmark using 600GB data][1]
* **Sequential read: cached situation offers a speed up of 2.78x over uncached situation.**
* **Random read (the data size read each time is 16MB): cached situation offers a speed up of 3.33x over uncached situation.**
* **Random read (the data size read each time is 4MB): cached situation offers a speed up of 4.76x over uncached situation.**
* Cached situation has lower performance degrade when running random read vs. sequential read.

### 2. Spark Terasort
![Spark Terasort benchmark using 300GB data][2]
* **Cached situation offers a speed up of 1.3x over  uncached situation.**

### 3. MapReduce Terasort
![MapReduce Terasort benchmark using 300GB data][3]
* **Cached situation gives a speed up of 1.22x over uncached situation.**
  


  [1]: ./images/1502345240892.jpg
  [2]: ./images/1502343336987.jpg
  [3]: ./images/1502347373327.jpg