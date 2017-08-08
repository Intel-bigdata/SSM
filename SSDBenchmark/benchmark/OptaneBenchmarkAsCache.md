Optane Benchmark As Cache
===============

ErasureCodeBenchmarkThroughput
-------------------------

### 1. Workload 
* Data size: 600GB
* Two reading types: sequential and random (in random read situation, 16MB data is read each time)

### 2. Test Result
![Throughput comparison][1]
* Cached situation offers 175% higher throughput than uncached situation.
* Cached situation has better performance when running random read: the throughput is 2.9% down in cached situation, and 17.9% down in uncached situation.

  [1]: ./images/1502176966922.jpg


  