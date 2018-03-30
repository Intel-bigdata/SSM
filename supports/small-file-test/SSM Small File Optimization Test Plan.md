SSM Small File Optimization Integration Test Plan
========================================

I. Hardware Configuration
----------------------------
Processor: Intel(R) Xeon(R) Gold 6140 CPU @ 2.30GHz
DRAM: DDR4 187GB
Network: 10GbE
Disks: 1 * 1TB SATA SSD, 6 * 1TB HDDs

II. Software Configuration
-----------------------------
1. Hadoop Configuration
Cluster: 1 NameNode + 3 DataNode
Data Disks: 6 * 1TB HDDs on each DataNode
Block Size: 128MB
Version: 2.7.3

2. SSM Configuration
1 * Server Node
3 * Agents

3. Other Configurations
MySQL: 5.7
Java: 1.8

### 1. Compact
#### Purpose
* Test the stability of small file compact rule and action.

#### Measurements
* Verify whether rule successfully executed.
* Execution time of compacting small files.

#### Data sets
* a. Small files: 10MB * [100, 500, 1000, 10000, 50000] files
* b. Tiny files: 1MB * [100, 500, 1000, 10000, 50000] files
* c. 10KB * [100, 500, 1000, 10000, 50000] files

#### Test cases
* a. Run small file compact rule only.
* b. Run small file compact rule and other rules like Data Mover, Data Sync simultaneously.
* c. Run small file compact rule, meanwhile, delete or append... the original small files.

### 2. Transparent Read

#### Purpose
* Test the availability and stability of transparent read to the upper-layer applications like HBase, Spark etc.

#### Measurements
* Verify whether tasks successfully executed.
* Execution time of tasks.

#### Data sets
* a. Small files: 10MB * [100, 500, 1000, 10000, 50000] files
* b. Tiny files: 1MB * [100, 500, 1000, 10000, 50000] files
* c. 10KB * [100, 500, 1000, 10000, 50000] files

#### Test cases
* a. Run hadoop bench of Hibench.
* b. Run spark bench of Hibench. 
